(ns yggdrasil.adapters.dolt-test
  "Compliance tests for the Dolt adapter.
   Tests are skipped when dolt is not available.

   To run: clj -M:test -n yggdrasil.adapters.dolt-test"
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.adapters.dolt :as dolt]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-dir [prefix]
  (str (Files/createTempDirectory prefix
                                  (make-array FileAttribute 0))))

(defn- make-fixture []
  {:create-system (fn []
                    (let [repo (temp-dir "ygg-dolt-")]
                      (dolt/init! repo {:system-name "test-dolt"})))
   :mutate (fn [sys]
             (dolt/write-entry! sys
                                (str "mutate-" (System/nanoTime))
                                (str (System/nanoTime)))
             sys)
   :commit (fn [sys msg]
             (p/commit! sys msg))
   :close! (fn [sys]
             (dolt/destroy! sys))
   :write-entry (fn [sys key value]
                  (dolt/write-entry! sys key value)
                  sys)
   :read-entry (fn [sys key]
                 (dolt/read-entry sys key))
   :count-entries (fn [sys]
                    (dolt/count-entries sys))
   :delete-entry (fn [sys key]
                   (dolt/delete-entry! sys key)
                   sys)
   :supports-concurrent? false})

;; ============================================================
;; Compliance tests
;; ============================================================

(deftest ^:dolt full-compliance-suite
  (if-not (dolt/dolt-available?)
    (println "SKIP: dolt not available")
    (testing "dolt adapter passes yggdrasil compliance suite"
      (compliance/run-compliance-tests (make-fixture)))))

(deftest ^:dolt system-identity-tests
  (when (dolt/dolt-available?)
    (let [fix (make-fixture)]
      (compliance/test-system-identity fix))))

(deftest ^:dolt snapshotable-tests
  (when (dolt/dolt-available?)
    (let [fix (make-fixture)]
      (compliance/test-snapshot-id-after-commit fix)
      (compliance/test-parent-ids-root-commit fix)
      (compliance/test-parent-ids-chain fix)
      (compliance/test-snapshot-meta fix))))

(deftest ^:dolt branchable-tests
  (when (dolt/dolt-available?)
    (let [fix (make-fixture)]
      (compliance/test-initial-branches fix)
      (compliance/test-create-branch fix)
      (compliance/test-checkout fix)
      (compliance/test-branch-isolation fix)
      (compliance/test-delete-branch fix))))

(deftest ^:dolt graphable-tests
  (when (dolt/dolt-available?)
    (let [fix (make-fixture)]
      (compliance/test-history fix)
      (compliance/test-history-limit fix)
      (compliance/test-ancestors fix)
      (compliance/test-ancestor-predicate fix))))

(deftest ^:dolt data-consistency-tests
  (when (dolt/dolt-available?)
    (let [fix (make-fixture)]
      (compliance/test-write-read-roundtrip fix)
      (compliance/test-count-after-writes fix)
      (compliance/test-multiple-entries-readable fix)
      (compliance/test-branch-data-isolation fix)
      (compliance/test-delete-entry-consistency fix)
      (compliance/test-overwrite-entry fix))))

(deftest ^:dolt merge-tests
  (when (dolt/dolt-available?)
    (let [fix (make-fixture)]
      (compliance/test-merge fix))))

(deftest ^:dolt dolt-sql-test
  (when (dolt/dolt-available?)
    (testing "SQL queries work correctly"
      (let [repo (temp-dir "ygg-dolt-sql-")
            sys (dolt/init! repo)]
        (try
          (dolt/write-entry! sys "key1" "value1")
          (dolt/write-entry! sys "key2" "value2")
          (is (= "value1" (dolt/read-entry sys "key1")))
          (is (= "value2" (dolt/read-entry sys "key2")))
          (is (= 2 (dolt/count-entries sys)))
          ;; REPLACE semantics (upsert)
          (dolt/write-entry! sys "key1" "updated")
          (is (= "updated" (dolt/read-entry sys "key1")))
          (is (= 2 (dolt/count-entries sys)))
          (finally
            (dolt/destroy! sys)))))))
