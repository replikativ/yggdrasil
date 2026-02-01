(ns yggdrasil.adapters.zfs-test
  "Compliance tests for the ZFS adapter.
   Tests are skipped when ZFS is not available or sudo requires a password."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.adapters.zfs :as zfs]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]))

;; ============================================================
;; ZFS availability check
;; ============================================================

(def ^:private zfs-pool
  "The ZFS dataset to use for tests. Override via ZFS_TEST_POOL env var.
   Default is rpool/yggdrasil (requires ZFS delegation, see README)."
  (or (System/getenv "ZFS_TEST_POOL") "rpool/yggdrasil"))

(defn- zfs-available?
  "Check if ZFS is usable (binary exists and passwordless sudo works)."
  []
  (try
    (let [result (sh "sudo" "-n" "zfs" "list" "-H" "-o" "name" zfs-pool)]
      (zero? (:exit result)))
    (catch Exception _ false)))

(defn- unique-prefix []
  (str "test-" (System/currentTimeMillis) "-" (rand-int 10000)))

(defn- zfs-cmd [& args]
  (let [result (apply sh "sudo" "zfs" args)]
    (when-not (zero? (:exit result))
      (throw (ex-info (str "zfs failed: " (:err result)) {:args args})))
    (str/trim (:out result))))

(defn- branch-mount
  "Get the mount path for the current branch of a ZFSSystem."
  [sys]
  (str (:mount-base sys) "/" (:prefix sys) "/" (:current-branch sys)))

(defn- make-fixture []
  {:create-system (fn []
                    (let [prefix (unique-prefix)]
                      (zfs/init! zfs-pool prefix {:system-name "test-zfs"})))
   :mutate (fn [sys]
             (let [mount (branch-mount sys)
                   f (str mount "/entries/mutate-" (System/nanoTime))]
               (spit f (str (System/nanoTime)))
               sys))
   :commit (fn [sys msg]
             (let [ds (str (:base-pool sys) "/" (:prefix sys) "/" (:current-branch sys))
                   snap-name (str (java.util.UUID/randomUUID))
                   full-snap (str ds "@" snap-name)]
               (zfs-cmd "snapshot" full-snap)
               (when (and msg (not (str/blank? msg)))
                 (try (zfs-cmd "set" (str "yggdrasil:message=" msg) full-snap)
                      (catch Exception _)))
               sys))
   :close! (fn [sys]
             (zfs/destroy! sys))
   :write-entry (fn [sys key value]
                  (let [mount (branch-mount sys)
                        f (str mount "/entries/" key)]
                    (spit f value)
                    sys))
   :read-entry (fn [sys key]
                 (let [mount (branch-mount sys)
                       f (java.io.File. (str mount "/entries/" key))]
                   (when (.exists f)
                     (slurp f))))
   :count-entries (fn [sys]
                    (let [mount (branch-mount sys)
                          dir (java.io.File. (str mount "/entries/"))]
                      (if (.exists dir)
                        (count (filter #(.isFile %) (.listFiles dir)))
                        0)))
   :delete-entry (fn [sys key]
                   (let [mount (branch-mount sys)
                         f (java.io.File. (str mount "/entries/" key))]
                     (when (.exists f)
                       (.delete f))
                     sys))
   :supports-concurrent? false})

;; ============================================================
;; Compliance tests (skipped when ZFS unavailable)
;; ============================================================

(deftest ^:zfs full-compliance-suite
  (if-not (zfs-available?)
    (println "SKIP: ZFS not available (set ZFS_TEST_POOL env var)")
    (testing "ZFS adapter passes yggdrasil compliance suite"
      (compliance/run-compliance-tests (make-fixture)))))

(deftest ^:zfs system-identity-tests
  (when (zfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-system-identity fix))))

(deftest ^:zfs snapshotable-tests
  (when (zfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-snapshot-id-after-commit fix)
      (compliance/test-parent-ids-root-commit fix)
      (compliance/test-parent-ids-chain fix)
      (compliance/test-snapshot-meta fix)
      (compliance/test-as-of fix))))

(deftest ^:zfs branchable-tests
  (when (zfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-initial-branches fix)
      (compliance/test-create-branch fix)
      (compliance/test-checkout fix)
      (compliance/test-branch-isolation fix)
      (compliance/test-delete-branch fix))))

(deftest ^:zfs graphable-tests
  (when (zfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-history fix)
      (compliance/test-history-limit fix)
      (compliance/test-ancestors fix)
      (compliance/test-ancestor-predicate fix))))

(deftest ^:zfs data-consistency-tests
  (when (zfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-write-read-roundtrip fix)
      (compliance/test-count-after-writes fix)
      (compliance/test-multiple-entries-readable fix)
      (compliance/test-branch-data-isolation fix)
      (compliance/test-as-of-data-consistency fix)
      (compliance/test-delete-entry-consistency fix)
      (compliance/test-overwrite-entry fix))))

(deftest ^:zfs watcher-test
  (when (zfs-available?)
    (testing "polling watcher detects snapshots"
      (let [prefix (unique-prefix)
            sys (zfs/init! zfs-pool prefix)]
        (try
          (let [events (atom [])
                watch-id (p/watch! sys (fn [e] (swap! events conj e))
                                   {:poll-interval-ms 100})]
            (try
              ;; Make a snapshot
              (let [ds (str zfs-pool "/" prefix "/main")
                    mount (branch-mount sys)]
                (spit (str mount "/entries/test") "hello")
                (zfs-cmd "snapshot" (str ds "@watch-test")))
              ;; Wait for poll to detect
              (Thread/sleep 300)
              (is (pos? (count @events))
                  "Should have detected the snapshot event")
              (finally
                (p/unwatch! sys watch-id))))
          (finally
            (zfs/destroy! sys)))))))
