(ns yggdrasil.adapters.lakefs-test
  "Compliance tests for the lakeFS adapter.
   Tests require a running lakeFS server.

   The test will attempt to start a local lakeFS server automatically.
   Set LAKEFS_ENDPOINT to skip auto-start and use an existing server.

   To run: clj -M:test -n yggdrasil.adapters.lakefs-test"
  (:require [clojure.test :refer [deftest testing use-fixtures]]
            [yggdrasil.adapters.lakefs :as lfs]
            [yggdrasil.compliance :as compliance]
            [clojure.java.shell :refer [sh]])
  (:import [java.io File]
           [java.net Socket]))

;; ============================================================
;; Server lifecycle management
;; ============================================================

(def ^:private server-process (atom nil))
(def ^:private test-config-path (atom nil))

(defn- port-open? [port]
  (try
    (let [s (Socket. "127.0.0.1" (int port))]
      (.close s)
      true)
    (catch Exception _ false)))

(defn- wait-for-port [port timeout-ms]
  (let [start (System/currentTimeMillis)]
    (loop []
      (cond
        (port-open? port) true
        (> (- (System/currentTimeMillis) start) timeout-ms) false
        :else (do (Thread/sleep 200) (recur))))))

(defn- find-binary [name]
  (let [home (System/getProperty "user.home")
        paths [(str home "/.local/bin/" name)
               (str "/usr/local/bin/" name)
               name]]
    (first (filter #(let [f (File. %)] (.exists f)) paths))))

(defn- setup-server! []
  ;; Check if server already running (e.g., user-provided)
  (if (port-open? 8000)
    true
    (let [lakefs-bin (find-binary "lakefs")]
      (when lakefs-bin
        ;; Clean previous data
        (let [home (System/getProperty "user.home")
              data-dir (File. (str home "/lakefs"))]
          (when (.exists data-dir)
            (doseq [f (reverse (sort-by #(.getPath %) (file-seq data-dir)))]
              (.delete f))))
        ;; Start server
        (let [pb (ProcessBuilder. [lakefs-bin "run" "--local-settings"])
              _ (.redirectErrorStream pb true)
              proc (.start pb)]
          (reset! server-process proc)
          ;; Wait for port
          (when (wait-for-port 8000 10000)
            ;; Setup credentials
            (Thread/sleep 1000)
            (let [result (sh "curl" "-s" "-X" "POST"
                             "http://127.0.0.1:8000/api/v1/setup_lakefs"
                             "-H" "Content-Type: application/json"
                             "-d" "{\"username\":\"admin\"}")]
              (when (zero? (:exit result))
                (let [output (:out result)
                      access-key (second (re-find #"\"access_key_id\":\"([^\"]+)\"" output))
                      secret-key (second (re-find #"\"secret_access_key\":\"([^\"]+)\"" output))]
                  (when (and access-key secret-key)
                    ;; Write lakectl config
                    (let [config-path (str (System/getProperty "java.io.tmpdir")
                                           "/ygg-lakefs-test.yaml")]
                      (spit config-path
                            (str "credentials:\n"
                                 "  access_key_id: " access-key "\n"
                                 "  secret_access_key: " secret-key "\n"
                                 "server:\n"
                                 "  endpoint_url: http://127.0.0.1:8000\n"))
                      (reset! test-config-path config-path)
                      true)))))))))))

(defn- teardown-server! []
  (when-let [proc @server-process]
    (.destroyForcibly proc)
    (reset! server-process nil))
  (when-let [config @test-config-path]
    (.delete (File. config))
    (reset! test-config-path nil)))

(defn- server-ready? []
  (or (port-open? 8000)
      (setup-server!)))

(defn server-fixture [f]
  (if (server-ready?)
    (binding [lfs/*lakectl-config* @test-config-path]
      (try
        (f)
        (finally
          (teardown-server!))))
    (do
      (println "SKIP: lakeFS server not available")
      (f))))

(use-fixtures :once server-fixture)

;; ============================================================
;; Test fixture
;; ============================================================

(defn- unique-repo-name []
  (str "ygg-test-" (System/nanoTime)))

(defn- make-fixture []
  (let [config @test-config-path]
    {:create-system (fn []
                      (binding [lfs/*lakectl-config* config]
                        (let [repo (unique-repo-name)]
                          (lfs/init! repo {:system-name (str "test-" repo)})
                          ;; Return system with config baked in
                          (assoc (lfs/create repo {:system-name (str "test-" repo)})
                                 :_config config))))
     :mutate (fn [sys]
               (binding [lfs/*lakectl-config* (:_config sys)]
                 (lfs/write-entry! sys
                                   (str "mutate-" (System/nanoTime))
                                   (str (System/nanoTime)))
                 sys))
     :commit (fn [sys msg]
               (binding [lfs/*lakectl-config* (:_config sys)]
                 (lfs/commit! sys msg)
                 sys))
     :close! (fn [sys]
               (binding [lfs/*lakectl-config* (:_config sys)]
                 (lfs/destroy! sys)))
     :write-entry (fn [sys key value]
                    (binding [lfs/*lakectl-config* (:_config sys)]
                      (lfs/write-entry! sys key value)
                      sys))
     :read-entry (fn [sys key]
                   (binding [lfs/*lakectl-config* (:_config sys)]
                     (lfs/read-entry sys key)))
     :count-entries (fn [sys]
                      (binding [lfs/*lakectl-config* (:_config sys)]
                        (lfs/count-entries sys)))
     :delete-entry (fn [sys key]
                     (binding [lfs/*lakectl-config* (:_config sys)]
                       (lfs/delete-entry! sys key)
                       sys))
     :supports-concurrent? false}))

;; ============================================================
;; Compliance tests
;; ============================================================

(deftest ^:lakefs full-compliance-suite
  (if-not @test-config-path
    (println "SKIP: lakeFS server not available (install lakefs binary or set LAKEFS_ENDPOINT)")
    (testing "lakefs adapter passes yggdrasil compliance suite"
      (compliance/run-compliance-tests (make-fixture)))))

(deftest ^:lakefs system-identity-tests
  (when @test-config-path
    (let [fix (make-fixture)]
      (compliance/test-system-identity fix))))

(deftest ^:lakefs snapshotable-tests
  (when @test-config-path
    (let [fix (make-fixture)]
      (compliance/test-snapshot-id-after-commit fix)
      (compliance/test-parent-ids-root-commit fix)
      (compliance/test-parent-ids-chain fix)
      (compliance/test-snapshot-meta fix))))

(deftest ^:lakefs branchable-tests
  (when @test-config-path
    (let [fix (make-fixture)]
      (compliance/test-initial-branches fix)
      (compliance/test-create-branch fix)
      (compliance/test-checkout fix)
      (compliance/test-branch-isolation fix)
      (compliance/test-delete-branch fix))))

(deftest ^:lakefs graphable-tests
  (when @test-config-path
    (let [fix (make-fixture)]
      (compliance/test-history fix)
      (compliance/test-history-limit fix)
      (compliance/test-ancestors fix)
      (compliance/test-ancestor-predicate fix))))

(deftest ^:lakefs data-consistency-tests
  (when @test-config-path
    (let [fix (make-fixture)]
      (compliance/test-write-read-roundtrip fix)
      (compliance/test-count-after-writes fix)
      (compliance/test-multiple-entries-readable fix)
      (compliance/test-branch-data-isolation fix)
      (compliance/test-delete-entry-consistency fix)
      (compliance/test-overwrite-entry fix))))

(deftest ^:lakefs merge-tests
  (when @test-config-path
    (let [fix (make-fixture)]
      (compliance/test-merge fix))))
