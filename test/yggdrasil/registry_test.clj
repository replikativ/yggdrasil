(ns yggdrasil.registry-test
  (:require [clojure.test :refer [deftest testing is are]]
            [yggdrasil.registry :as reg]
            [yggdrasil.storage :as store]
            [yggdrasil.types :as t])
  (:import [java.io File]))

;; ============================================================
;; Test helpers
;; ============================================================

(defn- make-entry
  "Create a RegistryEntry with reasonable defaults."
  [snap-id system-id branch-name physical-ts]
  (t/->RegistryEntry snap-id system-id branch-name
                     (t/->HLC physical-ts 0)
                     nil nil nil))

(defn- make-entry-with-meta
  [snap-id system-id branch-name physical-ts metadata]
  (t/->RegistryEntry snap-id system-id branch-name
                     (t/->HLC physical-ts 0)
                     nil nil metadata))

;; ============================================================
;; Registry CRUD tests
;; ============================================================

(deftest test-create-registry
  (testing "create in-memory registry"
    (let [r (reg/create-registry)]
      (is (= 0 (reg/entry-count r)))
      (is (empty? (reg/all-entries r)))
      (reg/close! r))))

(deftest test-register-and-count
  (testing "register entries increments count"
    (let [r (reg/create-registry)
          e1 (make-entry "snap-1" "git:repo1" "main" 1000)
          e2 (make-entry "snap-2" "git:repo1" "main" 2000)]
      (reg/register! r e1)
      (is (= 1 (reg/entry-count r)))
      (reg/register! r e2)
      (is (= 2 (reg/entry-count r)))
      (reg/close! r))))

(deftest test-deregister
  (testing "deregister removes entry from all indices"
    (let [r (reg/create-registry)
          e1 (make-entry "snap-1" "git:repo1" "main" 1000)
          e2 (make-entry "snap-2" "git:repo1" "main" 2000)]
      (reg/register! r e1)
      (reg/register! r e2)
      (is (= 2 (reg/entry-count r)))
      (reg/deregister! r e1)
      (is (= 1 (reg/entry-count r)))
      ;; Verify e1 is gone from all queries
      (is (nil? (reg/snapshot-refs r "snap-1")))
      (is (seq (reg/snapshot-refs r "snap-2")))
      (reg/close! r))))

(deftest test-register-batch
  (testing "register-batch adds multiple entries"
    (let [r (reg/create-registry)
          entries (mapv #(make-entry (str "snap-" %)
                                    "sys-1" "main"
                                    (* % 1000))
                        (range 1 6))]
      (reg/register-batch! r entries)
      (is (= 5 (reg/entry-count r)))
      (reg/close! r))))

;; ============================================================
;; TSBS index tests (temporal queries)
;; ============================================================

(deftest test-as-of
  (testing "as-of returns latest entry per system/branch at or before T"
    (let [r (reg/create-registry)
          e1 (make-entry "snap-1" "git:repo1" "main" 1000)
          e2 (make-entry "snap-2" "git:repo1" "main" 2000)
          e3 (make-entry "snap-3" "git:repo1" "main" 3000)
          e4 (make-entry "snap-a" "zfs:pool1" "main" 1500)
          e5 (make-entry "snap-b" "zfs:pool1" "main" 2500)]
      (reg/register-batch! r [e1 e2 e3 e4 e5])

      ;; At T=1500, git has snap-1, zfs has snap-a
      (let [world (reg/as-of r (t/->HLC 1500 0))]
        (is (= "snap-1" (:snapshot-id (get world ["git:repo1" "main"]))))
        (is (= "snap-a" (:snapshot-id (get world ["zfs:pool1" "main"])))))

      ;; At T=2500, git has snap-2, zfs has snap-b
      (let [world (reg/as-of r (t/->HLC 2500 0))]
        (is (= "snap-2" (:snapshot-id (get world ["git:repo1" "main"]))))
        (is (= "snap-b" (:snapshot-id (get world ["zfs:pool1" "main"])))))

      ;; At T=5000, everything is visible
      (let [world (reg/as-of r (t/->HLC 5000 0))]
        (is (= "snap-3" (:snapshot-id (get world ["git:repo1" "main"]))))
        (is (= "snap-b" (:snapshot-id (get world ["zfs:pool1" "main"])))))

      (reg/close! r))))

(deftest test-as-of-multi-branch
  (testing "as-of handles multiple branches per system"
    (let [r (reg/create-registry)
          e1 (make-entry "snap-1" "git:repo1" "main" 1000)
          e2 (make-entry "snap-2" "git:repo1" "feature" 1500)
          e3 (make-entry "snap-3" "git:repo1" "main" 2000)]
      (reg/register-batch! r [e1 e2 e3])

      (let [world (reg/as-of r (t/->HLC 1500 0))]
        (is (= "snap-1" (:snapshot-id (get world ["git:repo1" "main"]))))
        (is (= "snap-2" (:snapshot-id (get world ["git:repo1" "feature"])))))

      (reg/close! r))))

(deftest test-entries-in-range
  (testing "entries-in-range returns entries within HLC range"
    (let [r (reg/create-registry)
          entries (mapv #(make-entry (str "snap-" %)
                                    "sys-1" "main"
                                    (* % 1000))
                        (range 1 11))]
      (reg/register-batch! r entries)

      (let [result (reg/entries-in-range r (t/->HLC 3000 0) (t/->HLC 7000 0))]
        (is (= 5 (count result)))
        (is (= #{"snap-3" "snap-4" "snap-5" "snap-6" "snap-7"}
               (set (map :snapshot-id result)))))

      (reg/close! r))))

;; ============================================================
;; SBTS index tests (per-system history)
;; ============================================================

(deftest test-system-history
  (testing "system-history returns entries for a system/branch, newest first"
    (let [r (reg/create-registry)
          e1 (make-entry "snap-1" "git:repo1" "main" 1000)
          e2 (make-entry "snap-2" "git:repo1" "main" 2000)
          e3 (make-entry "snap-3" "git:repo1" "main" 3000)
          e4 (make-entry "snap-x" "zfs:pool1" "main" 1500)]
      (reg/register-batch! r [e1 e2 e3 e4])

      ;; Git history, newest first
      (let [history (reg/system-history r "git:repo1" "main")]
        (is (= 3 (count history)))
        (is (= ["snap-3" "snap-2" "snap-1"]
               (mapv :snapshot-id history))))

      ;; ZFS history
      (let [history (reg/system-history r "zfs:pool1" "main")]
        (is (= 1 (count history)))
        (is (= "snap-x" (:snapshot-id (first history)))))

      (reg/close! r))))

(deftest test-system-history-with-limit
  (testing "system-history respects :limit option"
    (let [r (reg/create-registry)
          entries (mapv #(make-entry (str "snap-" %)
                                    "git:repo1" "main"
                                    (* % 1000))
                        (range 1 11))]
      (reg/register-batch! r entries)

      (let [history (reg/system-history r "git:repo1" "main" {:limit 3})]
        (is (= 3 (count history)))
        ;; Newest first
        (is (= "snap-10" (:snapshot-id (first history)))))

      (reg/close! r))))

(deftest test-system-branches
  (testing "system-branches lists all branches for a system"
    (let [r (reg/create-registry)
          e1 (make-entry "snap-1" "git:repo1" "main" 1000)
          e2 (make-entry "snap-2" "git:repo1" "feature" 2000)
          e3 (make-entry "snap-3" "git:repo1" "develop" 3000)
          e4 (make-entry "snap-4" "zfs:pool1" "main" 1500)]
      (reg/register-batch! r [e1 e2 e3 e4])

      (is (= #{"main" "feature" "develop"}
             (reg/system-branches r "git:repo1")))
      (is (= #{"main"}
             (reg/system-branches r "zfs:pool1")))

      (reg/close! r))))

;; ============================================================
;; STBH index tests (GC ref-counting)
;; ============================================================

(deftest test-snapshot-refs
  (testing "snapshot-refs finds all references to a snapshot"
    (let [r (reg/create-registry)
          ;; Same snapshot referenced by two systems (e.g., after fork)
          e1 (make-entry "snap-1" "git:repo1" "main" 1000)
          e2 (make-entry "snap-1" "btrfs:vol1" "main" 1000)
          e3 (make-entry "snap-2" "git:repo1" "main" 2000)]
      (reg/register-batch! r [e1 e2 e3])

      (let [refs (reg/snapshot-refs r "snap-1")]
        (is (= 2 (count refs)))
        (is (= #{"git:repo1" "btrfs:vol1"}
               (set (map :system-id refs)))))

      (is (= 1 (count (reg/snapshot-refs r "snap-2"))))
      (is (nil? (reg/snapshot-refs r "snap-nonexistent")))

      (reg/close! r))))

(deftest test-snapshot-systems
  (testing "snapshot-systems returns set of system-ids"
    (let [r (reg/create-registry)
          e1 (make-entry "snap-1" "git:repo1" "main" 1000)
          e2 (make-entry "snap-1" "btrfs:vol1" "main" 1000)]
      (reg/register-batch! r [e1 e2])

      (is (= #{"git:repo1" "btrfs:vol1"}
             (reg/snapshot-systems r "snap-1")))

      (reg/close! r))))

;; ============================================================
;; Index consistency tests
;; ============================================================

(deftest test-index-consistency
  (testing "all three indices contain the same entries"
    (let [r (reg/create-registry)
          entries (mapv #(make-entry (str "snap-" %)
                                    (str "sys-" (mod % 3))
                                    (if (even? %) "main" "feature")
                                    (* % 1000))
                        (range 1 21))]
      (reg/register-batch! r entries)

      ;; All entries should be discoverable from any index
      (is (= 20 (reg/entry-count r)))

      ;; Verify via STBH: every snapshot should be findable
      (doseq [e entries]
        (is (seq (reg/snapshot-refs r (:snapshot-id e)))
            (str "Missing snapshot ref for " (:snapshot-id e))))

      ;; Deregister half, verify consistency
      (doseq [e (take 10 entries)]
        (reg/deregister! r e))

      (is (= 10 (reg/entry-count r)))

      ;; Remaining should still be findable
      (doseq [e (drop 10 entries)]
        (is (seq (reg/snapshot-refs r (:snapshot-id e)))
            (str "Missing snapshot ref for " (:snapshot-id e))))

      ;; Deregistered should be gone
      (doseq [e (take 10 entries)]
        (is (nil? (reg/snapshot-refs r (:snapshot-id e)))
            (str "Should be gone: " (:snapshot-id e))))

      (reg/close! r))))

;; ============================================================
;; HLC logical counter tests
;; ============================================================

(deftest test-hlc-ordering-in-indices
  (testing "entries at same physical time but different logical counters are ordered correctly"
    (let [r (reg/create-registry)
          e1 (t/->RegistryEntry "snap-1" "sys-1" "main" (t/->HLC 1000 0) nil nil nil)
          e2 (t/->RegistryEntry "snap-2" "sys-1" "main" (t/->HLC 1000 1) nil nil nil)
          e3 (t/->RegistryEntry "snap-3" "sys-1" "main" (t/->HLC 1000 2) nil nil nil)]
      (reg/register-batch! r [e1 e2 e3])

      (let [history (reg/system-history r "sys-1" "main")]
        (is (= ["snap-3" "snap-2" "snap-1"]
               (mapv :snapshot-id history))))

      (reg/close! r))))

;; ============================================================
;; Persistence round-trip tests
;; ============================================================

(defn- delete-recursive! [path]
  (let [dir (File. (str path))]
    (when (.exists dir)
      (doseq [f (reverse (sort-by #(.getPath %) (file-seq dir)))]
        (.delete f)))))

(defn- with-temp-store [f]
  (let [path (str "/tmp/yggdrasil-test-" (random-uuid))]
    (try
      (f path)
      (finally
        (delete-recursive! path)))))

(deftest test-persistence-round-trip
  (testing "registry survives close and reopen"
    (with-temp-store
      (fn [path]
        ;; Create registry, add entries, flush, close
        (let [r (reg/create-registry {:store-path path})
              e1 (make-entry "snap-1" "git:repo1" "main" 1000)
              e2 (make-entry "snap-2" "git:repo1" "main" 2000)
              e3 (make-entry "snap-a" "zfs:pool1" "main" 1500)]
          (reg/register-batch! r [e1 e2 e3])
          (reg/flush! r)
          (reg/close! r))

        ;; Reopen and verify entries are restored
        (let [r (reg/create-registry {:store-path path})]
          (is (= 3 (reg/entry-count r)))

          ;; TSBS: as-of query
          (let [world (reg/as-of r (t/->HLC 1500 0))]
            (is (= "snap-1" (:snapshot-id (get world ["git:repo1" "main"]))))
            (is (= "snap-a" (:snapshot-id (get world ["zfs:pool1" "main"])))))

          ;; SBTS: system history
          (let [history (reg/system-history r "git:repo1" "main")]
            (is (= 2 (count history)))
            (is (= ["snap-2" "snap-1"] (mapv :snapshot-id history))))

          ;; STBH: snapshot refs
          (is (= 1 (count (reg/snapshot-refs r "snap-1"))))
          (is (= 1 (count (reg/snapshot-refs r "snap-a"))))

          (reg/close! r))))))

(deftest test-persistence-incremental
  (testing "incremental updates persist correctly after flush"
    (with-temp-store
      (fn [path]
        ;; Phase 1: create and persist
        (let [r (reg/create-registry {:store-path path})]
          (reg/register! r (make-entry "snap-1" "sys-1" "main" 1000))
          (reg/register! r (make-entry "snap-2" "sys-1" "main" 2000))
          (reg/flush! r)
          (reg/close! r))

        ;; Phase 2: reopen, add more, flush
        (let [r (reg/create-registry {:store-path path})]
          (is (= 2 (reg/entry-count r)))
          (reg/register! r (make-entry "snap-3" "sys-1" "main" 3000))
          (reg/flush! r)
          (reg/close! r))

        ;; Phase 3: verify all 3 entries survive
        (let [r (reg/create-registry {:store-path path})]
          (is (= 3 (reg/entry-count r)))
          (let [history (reg/system-history r "sys-1" "main")]
            (is (= ["snap-3" "snap-2" "snap-1"]
                   (mapv :snapshot-id history))))
          (reg/close! r))))))

(deftest test-persistence-deregister
  (testing "deregistered entries are gone after flush/reopen"
    (with-temp-store
      (fn [path]
        (let [r (reg/create-registry {:store-path path})
              e1 (make-entry "snap-1" "sys-1" "main" 1000)
              e2 (make-entry "snap-2" "sys-1" "main" 2000)]
          (reg/register-batch! r [e1 e2])
          (reg/deregister! r e1)
          (reg/flush! r)
          (reg/close! r))

        (let [r (reg/create-registry {:store-path path})]
          (is (= 1 (reg/entry-count r)))
          (is (nil? (reg/snapshot-refs r "snap-1")))
          (is (seq (reg/snapshot-refs r "snap-2")))
          (reg/close! r))))))

(deftest test-persistence-freed-nodes
  (testing "markFreed tracks freed B-tree node addresses"
    (with-temp-store
      (fn [path]
        (let [r (reg/create-registry {:store-path path})]
          ;; Register entries to create B-tree nodes
          (reg/register-batch! r
                               (mapv #(make-entry (str "snap-" %)
                                                  "sys-1" "main"
                                                  (* % 1000))
                                     (range 1 101)))
          (reg/flush! r)

          ;; Deregister some — this triggers markFreed on old nodes
          (doseq [i (range 1 51)]
            (reg/deregister! r (make-entry (str "snap-" i)
                                           "sys-1" "main"
                                           (* i 1000))))
          (reg/flush! r)

          ;; The freed-atom in storage should have some addresses
          (when-let [storage (:storage r)]
            (is (pos? (count @(:freed-atom storage)))))

          (reg/close! r))))))

(deftest test-persistence-large-registry
  (testing "1000 entries persist and restore correctly"
    (with-temp-store
      (fn [path]
        (let [entries (mapv #(make-entry (str "snap-" %)
                                        (str "sys-" (mod % 5))
                                        (nth ["main" "dev" "staging"] (mod % 3))
                                        (* % 100))
                            (range 1 1001))]
          ;; Create and persist
          (let [r (reg/create-registry {:store-path path})]
            (reg/register-batch! r entries)
            (reg/flush! r)
            (reg/close! r))

          ;; Restore and verify
          (let [r (reg/create-registry {:store-path path})]
            (is (= 1000 (reg/entry-count r)))

            ;; Verify a temporal query
            (let [world (reg/as-of r (t/->HLC 50000 0))]
              (is (pos? (count world))))

            ;; Verify system history
            (let [history (reg/system-history r "sys-0" "dev")]
              (is (pos? (count history))))

            ;; Verify snapshot refs
            (is (seq (reg/snapshot-refs r "snap-500")))

            (reg/close! r)))))))
