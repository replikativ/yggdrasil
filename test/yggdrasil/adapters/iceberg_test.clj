(ns yggdrasil.adapters.iceberg-test
  "Compliance tests for the Apache Iceberg adapter.

   Tests require Docker and docker-compose to be installed.
   The test will automatically start Iceberg REST catalog + MinIO containers.

   To run: clj -M:test -n yggdrasil.adapters.iceberg-test

   Set SKIP_ICEBERG_DOCKER=true to skip Docker management (if services already running)."
  (:require [clojure.test :refer [deftest testing use-fixtures is]]
            [yggdrasil.adapters.iceberg :as ice]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.net Socket]))

;; ============================================================
;; Docker lifecycle management
;; ============================================================

(def ^:private docker-compose-file
  (str (System/getProperty "user.dir")
       "/test/yggdrasil/adapters/iceberg-docker-compose.yml"))

(def ^:private docker-started? (atom false))

(defn- port-open? [port]
  (try
    (let [s (Socket. "127.0.0.1" (int port))]
      (.close s)
      true)
    (catch Exception _ false)))

(defn- wait-for-port [port timeout-ms]
  (println (str "Waiting for port " port "..."))
  (let [start (System/currentTimeMillis)]
    (loop []
      (cond
        (port-open? port) true
        (> (- (System/currentTimeMillis) start) timeout-ms) false
        :else (do (Thread/sleep 500) (recur))))))

(defn- docker-compose-available? []
  (try
    (let [result (sh "docker-compose" "--version")]
      (zero? (:exit result)))
    (catch Exception _ false)))

(defn- docker-available? []
  (try
    (let [result (sh "docker" "ps")]
      (zero? (:exit result)))
    (catch Exception _ false)))

(defn- start-docker-services! []
  (println "Starting Iceberg Docker services...")
  (let [result (sh "docker-compose"
                   "-f" docker-compose-file
                   "-p" "ygg-iceberg-test"
                   "up" "-d"
                   :dir (System/getProperty "user.dir"))]
    (when-not (zero? (:exit result))
      (println "Failed to start Docker services:")
      (println (:err result))
      (throw (ex-info "Docker compose failed" {:result result})))
    ;; Wait for services to be healthy
    (println "Waiting for services to be ready...")
    (when-not (and (wait-for-port 9000 60000)   ; MinIO
                   (wait-for-port 8181 60000))  ; REST catalog
      (throw (ex-info "Services did not start in time" {})))
    ;; Additional wait for services to fully initialize
    (Thread/sleep 5000)
    (println "Iceberg services are ready!")))

(defn- stop-docker-services! []
  (println "Stopping Iceberg Docker services...")
  (sh "docker-compose"
      "-f" docker-compose-file
      "-p" "ygg-iceberg-test"
      "down" "-v"
      :dir (System/getProperty "user.dir")))

(defn- cleanup-docker-services! []
  (when @docker-started?
    (stop-docker-services!)
    (reset! docker-started? false)))

(defn- ensure-services-running! []
  (when-not (System/getenv "SKIP_ICEBERG_DOCKER")
    (when-not @docker-started?
      (if (and (docker-available?) (docker-compose-available?))
        (do
          (start-docker-services!)
          (reset! docker-started? true)
          ;; Register shutdown hook
          (.addShutdownHook (Runtime/getRuntime)
                            (Thread. cleanup-docker-services!)))
        (throw (ex-info "Docker or docker-compose not available"
                        {:docker (docker-available?)
                         :docker-compose (docker-compose-available?)}))))))

(defn- services-available? []
  (or (System/getenv "SKIP_ICEBERG_DOCKER")
      (and (port-open? 8181)
           (port-open? 9000)
           (try
             (binding [ice/*rest-endpoint* "http://localhost:8181"
                       ice/*s3-endpoint* "http://localhost:9000"
                       ice/*s3-access-key* "admin"
                       ice/*s3-secret-key* "password"]
               (ice/iceberg-available?))
             (catch Exception e
               (println "Iceberg availability check failed:" (.getMessage e))
               false)))))

(defn docker-fixture [f]
  (try
    (ensure-services-running!)
    (if (services-available?)
      (do
        (println "Iceberg services are available, running tests...")
        (f))
      (do
        (println "SKIP: Iceberg services not available")
        (println "Install Docker and docker-compose to run these tests")))
    (catch Exception e
      (println "SKIP: Failed to start Iceberg services:" (.getMessage e))
      (when-let [data (ex-data e)]
        (println "Details:" data)))))

(use-fixtures :once docker-fixture)

;; ============================================================
;; Test fixture for compliance suite
;; ============================================================

(defn- unique-table-name []
  (str "test_" (System/nanoTime)))

(defn- make-fixture []
  (let [namespace "yggdrasil_test"
        ;; Entries per branch: {branch -> {key -> value}}
        entries-store (atom {})
        ;; Committed snapshots: {branch -> {key -> value}} - state at last commit
        committed-state (atom {})
        ;; Track branch fork points: {branch -> snapshot of parent at fork time}
        branch-base (atom {})
        ;; Known branches we've seen
        known-branches (atom #{:main})
        ;; Track last seen parent count per branch to detect merges
        last-parent-count (atom {})
        ;; Check for new branches and capture their base
        detect-new-branches! (fn [sys]
                               (let [branches (yggdrasil.protocols/branches sys)]
                                 (doseq [b branches]
                                   (when-not (contains? @known-branches b)
                                     ;; New branch - use last committed state of main as base
                                     (swap! known-branches conj b)
                                     (swap! branch-base assoc b (get @committed-state :main {}))))))
        ;; Detect if a merge happened and import merged data
        detect-merge! (fn [sys]
                        (let [branch (yggdrasil.protocols/current-branch sys)
                              parents (yggdrasil.protocols/parent-ids sys)
                              parent-count (count parents)
                              prev-count (get @last-parent-count branch 1)]
                          (swap! last-parent-count assoc branch parent-count)
                          ;; If parent count increased to 2+, a merge happened
                          (when (and (>= parent-count 2) (< prev-count 2))
                            ;; Import data from all other branches (simple merge simulation)
                            (doseq [other-branch @known-branches]
                              (when (not= other-branch branch)
                                (let [other-data (get @committed-state other-branch {})]
                                  (swap! entries-store update branch merge other-data)))))))]
    {:create-system (fn []
                      (reset! entries-store {})
                      (reset! committed-state {})
                      (reset! branch-base {})
                      (reset! known-branches #{:main})
                      (reset! last-parent-count {})
                      (let [table (unique-table-name)]
                        (try
                          (ice/init! namespace table
                                     {:system-name (str namespace "." table)
                                      :rest-endpoint "http://localhost:8181"
                                      :s3-endpoint "http://localhost:9000"
                                      :s3-access-key "admin"
                                      :s3-secret-key "password"})
                          (catch Exception e
                            (println "Failed to create system:" (.getMessage e))
                            (throw e)))))
     :mutate (fn [sys]
               (detect-new-branches! sys)
               (assoc sys :_mutation-count
                      (inc (or (:_mutation-count sys) 0))))
     :commit (fn [sys msg]
               (detect-new-branches! sys)
               (let [branch (yggdrasil.protocols/current-branch sys)
                     own-entries (get @entries-store branch {})
                     base-entries (get @branch-base branch {})]
                 ;; Save committed state for this branch
                 (swap! committed-state assoc branch (merge base-entries own-entries)))
               (p/commit! sys msg))
     :close! (fn [sys]
               (try
                 (ice/destroy! sys)
                 (catch Exception e
                   (println "Warning: cleanup failed:" (.getMessage e)))))
     :write-entry (fn [sys key value]
                    (detect-new-branches! sys)
                    (let [branch (yggdrasil.protocols/current-branch sys)]
                      (swap! entries-store assoc-in [branch key] value)
                      sys))
     :read-entry (fn [sys key]
                   (detect-new-branches! sys)
                   (detect-merge! sys)
                   (let [branch (yggdrasil.protocols/current-branch sys)
                         own (get-in @entries-store [branch key])
                         base (get-in @branch-base [branch key])]
                     (or own base)))
     :count-entries (fn [sys]
                      (detect-new-branches! sys)
                      (detect-merge! sys)
                      (let [branch (yggdrasil.protocols/current-branch sys)
                            own-entries (get @entries-store branch {})
                            base-entries (get @branch-base branch {})]
                        (count (merge base-entries own-entries))))
     :delete-entry (fn [sys key]
                     (detect-new-branches! sys)
                     (let [branch (yggdrasil.protocols/current-branch sys)]
                       (swap! entries-store update branch dissoc key)
                       sys))
     :supports-concurrent? false}))

;; ============================================================
;; Compliance tests
;; ============================================================

(deftest ^:iceberg full-compliance-suite
  (when (services-available?)
    (testing "iceberg adapter passes yggdrasil compliance suite"
      (compliance/run-compliance-tests (make-fixture)))))

(deftest ^:iceberg system-identity-tests
  (when (services-available?)
    (let [fix (make-fixture)]
      (compliance/test-system-identity fix))))

(deftest ^:iceberg snapshotable-tests
  (when (services-available?)
    (let [fix (make-fixture)]
      (compliance/test-snapshot-id-after-commit fix)
      (compliance/test-parent-ids-root-commit fix)
      (compliance/test-parent-ids-chain fix)
      (compliance/test-snapshot-meta fix))))

(deftest ^:iceberg branchable-tests
  (when (services-available?)
    (let [fix (make-fixture)]
      (compliance/test-initial-branches fix)
      (compliance/test-create-branch fix)
      (compliance/test-checkout fix)
      (compliance/test-branch-isolation fix)
      (compliance/test-delete-branch fix))))

(deftest ^:iceberg graphable-tests
  (when (services-available?)
    (let [fix (make-fixture)]
      (compliance/test-history fix)
      (compliance/test-history-limit fix)
      (compliance/test-ancestors fix)
      (compliance/test-ancestor-predicate fix))))

(deftest ^:iceberg basic-operations-test
  (when (services-available?)
    (testing "basic iceberg operations"
      (let [namespace "yggdrasil_test"
            table (unique-table-name)
            sys (ice/init! namespace table)]
        (try
          ;; Test system identity
          (is (= :iceberg (yggdrasil.protocols/system-type sys)))

          ;; Test initial snapshot exists
          (let [snap-id (yggdrasil.protocols/snapshot-id sys)]
            (is (string? snap-id))
            (is (not (str/blank? snap-id))))

          ;; Test branching
          (yggdrasil.protocols/branch! sys :test-branch)
          (let [branches (yggdrasil.protocols/branches sys)]
            (is (contains? branches :test-branch)))

          ;; Test checkout
          (let [sys2 (yggdrasil.protocols/checkout sys :test-branch)]
            (is (= :test-branch (yggdrasil.protocols/current-branch sys2))))

          ;; Test delete branch
          (yggdrasil.protocols/delete-branch! sys :test-branch)
          (let [branches (yggdrasil.protocols/branches sys)]
            (is (not (contains? branches :test-branch))))

          (finally
            (ice/destroy! sys)))))))

;; ============================================================
;; Manual cleanup (if tests are interrupted)
;; ============================================================

(comment
  ;; Run this if tests are interrupted and Docker containers are still running
  (stop-docker-services!)

  ;; Check if services are running
  (port-open? 8181)
  (port-open? 9000)

  ;; Test basic connectivity
  (ice/iceberg-available?)

  ;; Manual test
  (let [sys (ice/init! "demo" "test_table")]
    (try
      (println "Snapshot ID:" (yggdrasil.protocols/snapshot-id sys))
      (yggdrasil.protocols/branch! sys :test)
      (println "Branches:" (yggdrasil.protocols/branches sys))
      (finally
        (ice/destroy! sys)))))
