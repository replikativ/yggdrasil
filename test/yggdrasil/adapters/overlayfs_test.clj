(ns yggdrasil.adapters.overlayfs-test
  "Compliance tests for the OverlayFS adapter."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.adapters.overlayfs :as ofs]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-dir [prefix]
  (str (Files/createTempDirectory prefix
                                  (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(defn- make-fixture []
  {:create-system (fn []
                    (let [base (temp-dir "ygg-ofs-base-")
                          ws (temp-dir "ygg-ofs-ws-")]
                      (ofs/init! base ws {:system-name "test-overlayfs"})))
   :mutate (fn [sys]
             (ofs/write-file! sys
                              (str "entries/mutate-" (System/nanoTime))
                              (str (System/nanoTime)))
             sys)
   :commit (fn [sys msg]
             (p/commit! sys msg))
   :close! (fn [sys]
             (ofs/destroy! sys)
             (delete-dir-recursive (:base-path sys)))
   :write-entry (fn [sys key value]
                  (ofs/write-file! sys (str "entries/" key) value)
                  sys)
   :read-entry (fn [sys key]
                 (ofs/read-file sys (str "entries/" key)))
   :count-entries (fn [sys]
                    (let [branch (:current-branch sys)
                          upper-entries (java.io.File.
                                         (str (:workspace-path sys) "/branches/"
                                              branch "/upper/entries"))
                          base-entries (java.io.File.
                                        (str (:base-path sys) "/entries"))
                          upper-files (if (.exists upper-entries)
                                        (set (map #(.getName %)
                                                  (filter #(.isFile %) (.listFiles upper-entries))))
                                        #{})
                          base-files (if (.exists base-entries)
                                       (set (map #(.getName %)
                                                 (filter #(.isFile %) (.listFiles base-entries))))
                                       #{})]
                      ;; Upper overrides base; empty files in upper = deleted from base
                      (count (clojure.set/union upper-files base-files))))
   :delete-entry (fn [sys key]
                   (ofs/delete-file! sys (str "entries/" key))
                   sys)
   :supports-concurrent? false})

;; ============================================================
;; Compliance tests
;; ============================================================

(deftest full-compliance-suite
  (testing "overlayfs adapter passes yggdrasil compliance suite"
    (compliance/run-compliance-tests (make-fixture))))

(deftest system-identity-tests
  (let [fix (make-fixture)]
    (compliance/test-system-identity fix)))

(deftest snapshotable-tests
  (let [fix (make-fixture)]
    (compliance/test-snapshot-id-after-commit fix)
    (compliance/test-parent-ids-root-commit fix)
    (compliance/test-parent-ids-chain fix)
    (compliance/test-snapshot-meta fix)
    (compliance/test-as-of fix)))

(deftest branchable-tests
  (let [fix (make-fixture)]
    (compliance/test-initial-branches fix)
    (compliance/test-create-branch fix)
    (compliance/test-checkout fix)
    (compliance/test-branch-isolation fix)
    (compliance/test-delete-branch fix)))

(deftest graphable-tests
  (let [fix (make-fixture)]
    (compliance/test-history fix)
    (compliance/test-history-limit fix)
    (compliance/test-ancestors fix)
    (compliance/test-ancestor-predicate fix)))

(deftest data-consistency-tests
  (let [fix (make-fixture)]
    (compliance/test-write-read-roundtrip fix)
    (compliance/test-count-after-writes fix)
    (compliance/test-multiple-entries-readable fix)
    (compliance/test-branch-data-isolation fix)
    (compliance/test-as-of-data-consistency fix)
    (compliance/test-delete-entry-consistency fix)
    (compliance/test-overwrite-entry fix)))

(deftest bubblewrap-exec-test
  (testing "exec! runs commands in sandbox"
    (let [base (temp-dir "ygg-ofs-exec-base-")
          ws (temp-dir "ygg-ofs-exec-ws-")
          sys (ofs/init! base ws)]
      (try
        ;; Write a file to the upper directory
        (ofs/write-file! sys "entries/hello" "world")
        ;; Execute a command that reads the file via overlay
        (let [result (ofs/exec! sys ["cat" (str base "/entries/hello")])]
          (is (zero? (:exit result))
              (str "exec should succeed, got: " (:err result)))
          (is (= "world" (:out result))
              "Should read file from overlay"))
        (finally
          (ofs/destroy! sys)
          (delete-dir-recursive base))))))

(deftest bubblewrap-isolation-test
  (testing "exec! provides network isolation"
    (let [base (temp-dir "ygg-ofs-iso-base-")
          ws (temp-dir "ygg-ofs-iso-ws-")
          sys (ofs/init! base ws)]
      (try
        ;; Network should be unavailable by default
        (let [result (ofs/exec! sys ["ping" "-c" "1" "-W" "1" "8.8.8.8"])]
          (is (not (zero? (:exit result)))
              "Network should be blocked by default"))
        (finally
          (ofs/destroy! sys)
          (delete-dir-recursive base))))))
