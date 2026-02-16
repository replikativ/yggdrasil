(ns yggdrasil.adapters.btrfs-test
  "Compliance tests for the Btrfs adapter.
   Tests auto-detect btrfs mounts, or use BTRFS_TEST_PATH if set.

   To run: clj -M:test -n yggdrasil.adapters.btrfs-test

   The test path must be on a Btrfs filesystem where the user can create subvolumes
   (mount with user_subvol_rm_allowed, or set *use-sudo* to true)."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [yggdrasil.adapters.btrfs :as btrfs]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p])
  (:import [java.io File]))

;; ============================================================
;; Btrfs availability check
;; ============================================================

(defn- find-btrfs-mount
  "Scan /proc/mounts to find a writable btrfs mount point."
  []
  (try
    (with-open [reader (java.io.BufferedReader. (java.io.FileReader. "/proc/mounts"))]
      ;; Force full evaluation inside with-open by realizing to vec first
      (-> (line-seq reader)
          vec
          (->> (keep #(let [parts (str/split % #"\s+")]
                        (when (= "btrfs" (nth parts 2 nil))
                          (second parts))))
               (filter #(let [f (File. %)]
                          (and (.exists f)
                               (.canWrite f))))
               first)))
    (catch Exception _ nil)))

(defn- btrfs-test-path
  "Get the Btrfs test path from environment variable or auto-detect."
  []
  (or (System/getenv "BTRFS_TEST_PATH")
      (find-btrfs-mount)))

(defn- btrfs-available?
  "Check if btrfs CLI is available and the test path is on Btrfs."
  []
  (when-let [path (btrfs-test-path)]
    (and (.exists (File. path))
         (btrfs/btrfs-available? path))))

(defn- test-base-path
  "Create a unique test directory under the Btrfs test path."
  []
  (let [base (str (btrfs-test-path) "/ygg-btrfs-test-" (System/nanoTime))]
    (.mkdirs (File. base))
    base))

(defn- make-fixture []
  {:create-system (fn []
                    (let [base (test-base-path)]
                      (btrfs/init! base {:system-name "test-btrfs"})))
   :mutate (fn [sys]
             (btrfs/write-file! sys
                                (str "mutate-" (System/nanoTime))
                                (str (System/nanoTime)))
             sys)
   :commit (fn [sys msg]
             (p/commit! sys msg))
   :close! (fn [sys]
             (btrfs/destroy! sys))
   :write-entry (fn [sys key value]
                  (btrfs/write-file! sys key value)
                  sys)
   :read-entry (fn [sys key]
                 (btrfs/read-file sys key))
   :count-entries (fn [sys]
                    (let [branch (:current-branch sys)
                          entries-dir (File. (str (:base-path sys) "/branches/"
                                                  branch "/entries"))]
                      (if (.exists entries-dir)
                        (count (filter #(.isFile %) (.listFiles entries-dir)))
                        0)))
   :delete-entry (fn [sys key]
                   (btrfs/delete-file! sys key)
                   sys)
   :supports-concurrent? false})

;; ============================================================
;; Compliance tests (skipped when btrfs unavailable)
;; ============================================================

(deftest ^:btrfs full-compliance-suite
  (if-not (btrfs-available?)
    (println "SKIP: btrfs not available (set BTRFS_TEST_PATH to a directory on Btrfs)")
    (testing "btrfs adapter passes yggdrasil compliance suite"
      (compliance/run-compliance-tests (make-fixture)))))

(deftest ^:btrfs system-identity-tests
  (when (btrfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-system-identity fix))))

(deftest ^:btrfs snapshotable-tests
  (when (btrfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-snapshot-id-after-commit fix)
      (compliance/test-parent-ids-root-commit fix)
      (compliance/test-parent-ids-chain fix)
      (compliance/test-snapshot-meta fix))))

(deftest ^:btrfs branchable-tests
  (when (btrfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-initial-branches fix)
      (compliance/test-create-branch fix)
      (compliance/test-checkout fix)
      (compliance/test-branch-isolation fix)
      (compliance/test-delete-branch fix))))

(deftest ^:btrfs graphable-tests
  (when (btrfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-history fix)
      (compliance/test-history-limit fix)
      (compliance/test-ancestors fix)
      (compliance/test-ancestor-predicate fix))))

(deftest ^:btrfs data-consistency-tests
  (when (btrfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-write-read-roundtrip fix)
      (compliance/test-count-after-writes fix)
      (compliance/test-multiple-entries-readable fix)
      (compliance/test-branch-data-isolation fix)
      (compliance/test-delete-entry-consistency fix)
      (compliance/test-overwrite-entry fix))))

(deftest ^:btrfs merge-tests
  (when (btrfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-merge fix))))

(deftest ^:btrfs pruning-test
  (when (btrfs-available?)
    (testing "automatic snapshot pruning"
      (let [base (test-base-path)
            sys (btrfs/init! base {:max-snapshots 3})]
        (try
          ;; Create 5 commits - should prune to keep only last 3 snapshots
          (dotimes [i 5]
            (btrfs/write-file! sys (str "file-" i) (str "value-" i))
            (btrfs/commit! sys (str "commit " i)))
          ;; All 5 commits should be in history
          (is (= 5 (count (p/history sys))))
          ;; But only 3 snapshot subvolumes should exist
          (let [snap-dir (File. (str base "/snapshots/main"))
                snap-count (count (filter #(.isDirectory %) (.listFiles snap-dir)))]
            (is (<= snap-count 3)
                (str "Should have at most 3 snapshots, got " snap-count)))
          (finally
            (btrfs/destroy! sys)))))))
