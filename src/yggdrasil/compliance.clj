(ns yggdrasil.compliance
  "Protocol-agnostic compliance test suite for Yggdrasil adapters.

  All protocols use VALUE SEMANTICS: mutating operations return new system
  values. Tests thread system values through operations.

  Each adapter provides a fixture map with:
    :create-system  - (fn [] system) creates a fresh system with 'main' branch
    :mutate         - (fn [system] new-system) performs a mutation, returns new system
    :commit         - (fn [system msg] new-system) commits, returns new system
                      (snapshot-id available via (p/snapshot-id new-system))
    :close!         - (fn [system] ...) closes/cleans up the system
    :write-entry    - (fn [system key value] new-system) writes a keyed entry
    :read-entry     - (fn [system key] value-or-nil) reads entry by key
    :count-entries  - (fn [system] n) counts all entries in current state
    :delete-entry   - (fn [system key] new-system) deletes entry by key (nil if unsupported)

  Tests for protocol layers (branchable, graphable, mergeable) are skipped
  automatically when the system's capabilities indicate non-support.

  Usage from adapter test ns:
    (deftest compliance
      (compliance/run-compliance-tests my-fixture-map))"
  (:require [clojure.test :refer [is testing]]
            [yggdrasil.protocols :as p]))

;; ============================================================
;; Helpers
;; ============================================================

(defn- has-capability?
  "Check if the system supports a given capability layer.
   Creates a system, checks capabilities, then closes."
  [{:keys [create-system close!]} cap-key]
  (let [sys (create-system)]
    (try
      (get (p/capabilities sys) cap-key false)
      (finally (close! sys)))))

;; ============================================================
;; Layer 1: Snapshotable tests
;; ============================================================

(defn test-snapshot-id-after-commit [{:keys [create-system mutate commit close!]}]
  (testing "snapshot-id returns the current commit ID after a commit"
    (let [sys (create-system)]
      (try
        (let [sys (mutate sys)
              sys (commit sys "first")
              sid (p/snapshot-id sys)]
          (is (string? sid)
              "snapshot-id should return a string"))
        (finally (close! sys))))))

(defn test-parent-ids-root-commit [{:keys [create-system mutate commit close!]}]
  (testing "root commit has empty parent-ids"
    (let [sys (create-system)]
      (try
        (let [sys (-> sys mutate (commit "root"))]
          (is (empty? (p/parent-ids sys))
              "First commit should have no parents"))
        (finally (close! sys))))))

(defn test-parent-ids-chain [{:keys [create-system mutate commit close!]}]
  (testing "second commit has first commit as parent"
    (let [sys (create-system)]
      (try
        (let [sys (-> sys mutate (commit "first"))
              first-id (p/snapshot-id sys)
              sys (-> sys mutate (commit "second"))]
          (is (contains? (p/parent-ids sys) first-id)
              "Second commit should have first commit as parent"))
        (finally (close! sys))))))

(defn test-snapshot-meta [{:keys [create-system mutate commit close!]}]
  (testing "snapshot-meta returns metadata for a commit"
    (let [sys (create-system)]
      (try
        (let [sys (-> sys mutate (commit "test message"))
              sid (p/snapshot-id sys)
              meta (p/snapshot-meta sys sid)]
          (is (some? meta) "snapshot-meta should return non-nil")
          (is (= sid (:snapshot-id meta)) "meta should contain snapshot-id")
          (is (set? (:parent-ids meta)) "meta should contain parent-ids set"))
        (finally (close! sys))))))

(defn test-as-of [{:keys [create-system mutate commit close!]}]
  (testing "as-of returns a read-only view at a snapshot"
    (let [sys (create-system)]
      (try
        (let [sys (-> sys mutate (commit "snapshot point"))
              sid (p/snapshot-id sys)
              view (p/as-of sys sid)]
          (is (some? view) "as-of should return non-nil"))
        (finally (close! sys))))))

;; ============================================================
;; Layer 2: Branchable tests
;; ============================================================

(defn test-initial-branches [{:keys [create-system close!] :as fix}]
  (when (has-capability? fix :branchable)
    (testing "fresh system has a main branch"
      (let [sys (create-system)]
        (try
          (is (contains? (p/branches sys) :main)
              "Should have :main branch")
          (is (= :main (p/current-branch sys))
              "Current branch should be :main")
          (finally (close! sys)))))))

(defn test-create-branch [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :branchable)
    (testing "branch! creates a new branch and returns new system"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "before fork"))
                sys (p/branch! sys :experiment)]
            (is (contains? (p/branches sys) :experiment)
                "Should have :experiment branch after branching")
            (is (= :main (p/current-branch sys))
                "branch! should not switch current branch"))
          (finally (close! sys)))))))

(defn test-checkout [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :branchable)
    (testing "checkout returns new system on target branch"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "before fork"))
                sys (p/branch! sys :experiment)
                sys (p/checkout sys :experiment)]
            (is (= :experiment (p/current-branch sys))
                "current-branch should be :experiment after checkout"))
          (finally (close! sys)))))))

(defn test-branch-isolation [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :branchable)
    (testing "commits on one branch don't affect another"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "main commit"))
                sys (p/branch! sys :experiment)
                main-after-fork (p/snapshot-id sys)
                ;; Advance experiment
                exp-sys (-> sys
                            (p/checkout :experiment)
                            mutate
                            (commit "experiment commit"))
                ;; Main should still be at fork point
                main-sys (p/checkout exp-sys :main)]
            (is (= main-after-fork (p/snapshot-id main-sys))
                "main should still be at its fork-point commit"))
          (finally (close! sys)))))))

(defn test-delete-branch [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :branchable)
    (testing "delete-branch! returns system without the branch"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "before fork"))
                sys (p/branch! sys :temp)
                _ (is (contains? (p/branches sys) :temp))
                sys (p/delete-branch! sys :temp)]
            (is (not (contains? (p/branches sys) :temp))
                "Branch should be gone after delete"))
          (finally (close! sys)))))))

;; ============================================================
;; Layer 3: Graphable tests
;; ============================================================

(defn test-history [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :graphable)
    (testing "history returns commit IDs newest first"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "first"))
                sys (-> sys mutate (commit "second"))
                sys (-> sys mutate (commit "third"))
                id3 (p/snapshot-id sys)
                hist (p/history sys)]
            (is (= id3 (first hist))
                "Most recent commit should be first")
            (is (>= (count hist) 3)
                "Should have at least 3 commits"))
          (finally (close! sys)))))))

(defn test-history-limit [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :graphable)
    (testing "history respects :limit option"
      (let [sys (create-system)]
        (try
          (let [sys (reduce (fn [s _] (-> s mutate (commit "commit")))
                            sys (range 5))
                hist (p/history sys {:limit 2})]
            (is (= 2 (count hist))
                "Should return only 2 commits with :limit 2"))
          (finally (close! sys)))))))

(defn test-ancestors [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :graphable)
    (testing "ancestors returns all ancestor IDs"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "first"))
                id1 (p/snapshot-id sys)
                sys (-> sys mutate (commit "second"))
                id2 (p/snapshot-id sys)
                sys (-> sys mutate (commit "third"))
                id3 (p/snapshot-id sys)
                ancs (set (p/ancestors sys id3))]
            (is (contains? ancs id2)
                "id2 should be ancestor of id3")
            (is (contains? ancs id1)
                "id1 should be ancestor of id3"))
          (finally (close! sys)))))))

(defn test-ancestor-predicate [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :graphable)
    (testing "ancestor? checks ancestry relationship"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "first"))
                id1 (p/snapshot-id sys)
                sys (-> sys mutate (commit "second"))
                id2 (p/snapshot-id sys)]
            (is (true? (p/ancestor? sys id1 id2))
                "id1 should be ancestor of id2")
            (is (false? (p/ancestor? sys id2 id1))
                "id2 should NOT be ancestor of id1"))
          (finally (close! sys)))))))

(defn test-common-ancestor [{:keys [create-system mutate commit close!] :as fix}]
  (when (and (has-capability? fix :graphable) (has-capability? fix :branchable))
    (testing "common-ancestor finds merge base of diverged branches"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "common base"))
                sys (p/branch! sys :feature)
                fork-point (p/snapshot-id sys)
                ;; Advance main
                main-sys (-> sys mutate (commit "main advance"))
                main-id (p/snapshot-id main-sys)
                ;; Advance feature
                feat-sys (-> sys
                             (p/checkout :feature)
                             mutate
                             (commit "feature advance"))
                feat-id (p/snapshot-id feat-sys)
                ;; Check common ancestor from either system
                ancestor (p/common-ancestor main-sys main-id feat-id)]
            (is (= fork-point ancestor)
                "Common ancestor should be the fork point"))
          (finally (close! sys)))))))

(defn test-commit-graph [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :graphable)
    (testing "commit-graph returns full DAG structure"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "first"))
                sys (-> sys mutate (commit "second"))
                graph (p/commit-graph sys)]
            (is (map? (:nodes graph)) "Should have :nodes map")
            (is (map? (:branches graph)) "Should have :branches map")
            (is (set? (:roots graph)) "Should have :roots set")
            (is (pos? (count (:nodes graph))) "Should have at least 1 node")
            (is (pos? (count (:roots graph))) "Should have at least 1 root"))
          (finally (close! sys)))))))

(defn test-commit-info [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :graphable)
    (testing "commit-info returns metadata for a specific commit"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "test info"))
                sid (p/snapshot-id sys)
                info (p/commit-info sys sid)]
            (is (some? info) "commit-info should return non-nil")
            (is (set? (:parent-ids info)) "Should have :parent-ids")
            (is (some? (:timestamp info)) "Should have :timestamp"))
          (finally (close! sys)))))))

;; ============================================================
;; Layer 4: Mergeable tests
;; ============================================================

(defn test-merge [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :mergeable)
    (testing "merge! returns new system with merge applied"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "base"))
                sys (p/branch! sys :feature)
                sys (-> sys (p/checkout :feature) mutate (commit "feature work"))
                ;; Merge feature into main
                sys (p/checkout sys :main)
                sys (p/merge! sys :feature)
                merge-id (p/snapshot-id sys)]
            (is (string? merge-id)
                "merge result should have a snapshot-id"))
          (finally (close! sys)))))))

(defn test-merge-parent-ids [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :mergeable)
    (testing "merge commit has two parents"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "base"))
                sys (p/branch! sys :feature)
                ;; Advance main
                sys (-> sys mutate (commit "main advance"))
                ;; Advance feature
                sys (-> sys (p/checkout :feature) mutate (commit "feature advance"))
                ;; Switch back to main and merge feature
                sys (p/checkout sys :main)
                sys (p/merge! sys :feature)
                parents (p/parent-ids sys)]
            (is (>= (count parents) 2)
                "Merge commit should have at least 2 parents"))
          (finally (close! sys)))))))

(defn test-conflicts-empty-for-compatible [{:keys [create-system mutate commit close!] :as fix}]
  (when (has-capability? fix :mergeable)
    (testing "conflicts returns empty for compatible branches"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys mutate (commit "base"))
                sys (p/branch! sys :feature)
                main-sys (-> sys mutate (commit "main"))
                main-id (p/snapshot-id main-sys)
                feat-sys (-> sys (p/checkout :feature) mutate (commit "feature"))
                feat-id (p/snapshot-id feat-sys)
                conflicts (p/conflicts main-sys main-id feat-id)]
            (is (empty? conflicts)
                "Compatible branches should have no conflicts"))
          (finally (close! sys)))))))

;; ============================================================
;; SystemIdentity tests
;; ============================================================

(defn test-system-identity [{:keys [create-system close!]}]
  (testing "system identity is properly configured"
    (let [sys (create-system)]
      (try
        (is (string? (p/system-id sys))
            "system-id should return a string")
        (is (keyword? (p/system-type sys))
            "system-type should return a keyword")
        (let [caps (p/capabilities sys)]
          (is (some? caps) "capabilities should return non-nil")
          (is (true? (:snapshotable caps))
              "should support snapshotable"))
        (finally (close! sys))))))

;; ============================================================
;; Concurrent stress tests
;; ============================================================

(defn test-concurrent-commits [{:keys [create-system mutate commit close!] :as fix}]
  (when (get fix :supports-concurrent? true)
    (testing "concurrent commits on same branch are serialized safely"
      (let [sys (create-system)
            sys (-> sys mutate (commit "initial"))
            sys-atom (atom sys)
            n-threads 8
            n-commits-per-thread 10
            errors (atom [])
            latch (java.util.concurrent.CountDownLatch. n-threads)]
        (try
          ;; Launch concurrent writers
          (dotimes [t n-threads]
            (.start
             (Thread.
              (fn []
                (try
                  (dotimes [i n-commits-per-thread]
                    (swap! sys-atom (fn [s] (-> s mutate (commit (str "thread-" t "-commit-" i))))))
                  (catch Throwable e
                    (swap! errors conj {:thread t :error e}))
                  (finally
                    (.countDown latch)))))))
          (.await latch 30 java.util.concurrent.TimeUnit/SECONDS)
          (is (empty? @errors)
              (str "No errors expected during concurrent commits, got: "
                   (mapv #(-> % :error .getMessage) @errors)))
          ;; Verify history has a reasonable number of commits
          (let [hist (p/history @sys-atom)]
            (is (>= (count hist) n-threads)
                "History should have at least one commit per thread"))
          (finally (close! @sys-atom)))))))

(defn test-concurrent-branch-creation [{:keys [create-system mutate commit close!] :as fix}]
  (when (and (has-capability? fix :branchable) (get fix :supports-concurrent? true))
    (testing "concurrent branch creation doesn't corrupt state"
      (let [sys (create-system)
            sys (-> sys mutate (commit "base for branches"))
            sys-atom (atom sys)
            n-branches 10
            errors (atom [])
            latch (java.util.concurrent.CountDownLatch. n-branches)]
        (try
          ;; Create branches concurrently
          (dotimes [i n-branches]
            (.start
             (Thread.
              (fn []
                (try
                  (swap! sys-atom (fn [s] (p/branch! s (keyword (str "branch-" i)))))
                  (catch Throwable e
                    (swap! errors conj {:branch i :error e}))
                  (finally
                    (.countDown latch)))))))
          (.await latch 30 java.util.concurrent.TimeUnit/SECONDS)
          (is (empty? @errors)
              (str "No errors during concurrent branch creation: "
                   (mapv #(-> % :error .getMessage) @errors)))
          ;; All branches should exist
          (let [bs (p/branches @sys-atom)]
            (is (>= (count bs) (inc n-branches))
                "All branches plus main should exist"))
          (finally (close! @sys-atom)))))))

(defn test-concurrent-readers [{:keys [create-system mutate commit close!] :as fix}]
  (when (get fix :supports-concurrent? true)
    (testing "concurrent readers while writer is active"
      (let [sys (create-system)
            sys (-> sys mutate (commit "initial"))
            sys-atom (atom sys)
            n-readers 8
            n-reads-per-reader 20
            errors (atom [])
            writer-done (atom false)
            latch (java.util.concurrent.CountDownLatch. (inc n-readers))]
        (try
          ;; Writer thread
          (.start
           (Thread.
            (fn []
              (try
                (dotimes [i 20]
                  (swap! sys-atom (fn [s] (-> s mutate (commit (str "write-" i))))))
                (catch Throwable e
                  (swap! errors conj {:role :writer :error e}))
                (finally
                  (reset! writer-done true)
                  (.countDown latch))))))
          ;; Reader threads
          (dotimes [r n-readers]
            (.start
             (Thread.
              (fn []
                (try
                  (dotimes [_ n-reads-per-reader]
                    (let [s @sys-atom
                          sid (p/snapshot-id s)]
                      (when sid
                        (let [meta (p/snapshot-meta s sid)]
                          (is (some? meta) "snapshot-meta should succeed during writes"))))
                    (Thread/sleep 1))
                  (catch Throwable e
                    (swap! errors conj {:role :reader :thread r :error e}))
                  (finally
                    (.countDown latch)))))))
          (.await latch 30 java.util.concurrent.TimeUnit/SECONDS)
          (is (empty? @errors)
              (str "No errors during concurrent read/write: "
                   (mapv #(-> % :error .getMessage) @errors)))
          (finally (close! @sys-atom)))))))

;; ============================================================
;; Data Consistency tests
;; ============================================================

(defn test-write-read-roundtrip
  [{:keys [create-system write-entry read-entry commit close!]}]
  (testing "write then read returns the written value"
    (let [sys (create-system)]
      (try
        (let [sys (-> sys
                      (write-entry "key-1" "value-alpha")
                      (commit "write one entry"))]
          (is (= "value-alpha" (read-entry sys "key-1"))
              "Should read back the written value"))
        (finally (close! sys))))))

(defn test-count-after-writes
  [{:keys [create-system write-entry count-entries commit close!]}]
  (testing "count-entries increases after writes and commits"
    (let [sys (create-system)]
      (try
        (is (= 0 (count-entries sys))
            "Fresh system should have 0 entries")
        (let [sys (-> sys (write-entry "a" "1") (commit "first"))]
          (is (= 1 (count-entries sys)))
          (let [sys (-> sys
                        (write-entry "b" "2")
                        (write-entry "c" "3")
                        (commit "second"))]
            (is (= 3 (count-entries sys)))))
        (finally (close! sys))))))

(defn test-multiple-entries-readable
  [{:keys [create-system write-entry read-entry commit close!]}]
  (testing "multiple entries are independently readable"
    (let [sys (create-system)]
      (try
        (let [sys (-> sys
                      (write-entry "x" "val-x")
                      (write-entry "y" "val-y")
                      (write-entry "z" "val-z")
                      (commit "three entries"))]
          (is (= "val-x" (read-entry sys "x")))
          (is (= "val-y" (read-entry sys "y")))
          (is (= "val-z" (read-entry sys "z")))
          (is (nil? (read-entry sys "nonexistent"))
              "Non-existent key should return nil"))
        (finally (close! sys))))))

(defn test-branch-data-isolation
  [{:keys [create-system write-entry read-entry count-entries commit close!] :as fix}]
  (when (has-capability? fix :branchable)
    (testing "data written on one branch is not visible on another"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys (write-entry "shared" "base-value") (commit "base commit"))
                sys (p/branch! sys :feature)
                ;; Write on main
                sys (-> sys (write-entry "main-only" "main-data") (commit "main write"))
                ;; Write on feature
                sys (-> sys
                        (p/checkout :feature)
                        (write-entry "feature-only" "feature-data")
                        (commit "feature write"))]
            ;; Verify feature isolation (currently on feature)
            (is (= "feature-data" (read-entry sys "feature-only"))
                "Feature branch should see its own data")
            (is (nil? (read-entry sys "main-only"))
                "Feature branch should NOT see main-only data")
            (is (= "base-value" (read-entry sys "shared"))
                "Feature should see shared base data")
            ;; Switch to main and verify its isolation
            (let [sys (p/checkout sys :main)]
              (is (= "main-data" (read-entry sys "main-only"))
                  "Main branch should see its own data")
              (is (nil? (read-entry sys "feature-only"))
                  "Main branch should NOT see feature-only data")
              (is (= "base-value" (read-entry sys "shared"))
                  "Main should see shared base data")))
          (finally (close! sys)))))))

(defn test-merge-data-visibility
  [{:keys [create-system write-entry read-entry count-entries commit close!] :as fix}]
  (when (has-capability? fix :mergeable)
    (testing "after merge, data from source branch is visible on target"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys (write-entry "base" "base-val") (commit "base"))
                sys (p/branch! sys :feature)
                ;; Add data on feature
                sys (-> sys
                        (p/checkout :feature)
                        (write-entry "feat-entry" "feat-val")
                        (commit "feature data"))
                ;; Switch back to main and merge feature
                sys (p/checkout sys :main)
                pre-merge-count (count-entries sys)
                sys (p/merge! sys :feature)]
            ;; After merge, main should have feature's data
            (is (= "feat-val" (read-entry sys "feat-entry"))
                "Main should see feature data after merge")
            (is (> (count-entries sys) pre-merge-count)
                "Entry count should increase after merge"))
          (finally (close! sys)))))))

(defn test-as-of-data-consistency
  [{:keys [create-system write-entry read-entry count-entries commit close!]}]
  (testing "as-of snapshot shows data consistent with that point in time"
    (let [sys (create-system)]
      (try
        (let [sys (-> sys (write-entry "first" "v1") (commit "first entry"))
              snap1 (p/snapshot-id sys)
              sys (-> sys (write-entry "second" "v2") (commit "second entry"))
              snap2 (p/snapshot-id sys)]
          ;; Current state has both
          (is (= 2 (count-entries sys)))
          ;; as-of snap1 should exist
          (let [view1 (p/as-of sys snap1)]
            (is (some? view1) "as-of should return a view")
            ;; If the view is a full system, verify data consistency
            (when (satisfies? p/SystemIdentity view1)
              (is (= 1 (count-entries view1))
                  "Historical view at snap1 should have 1 entry")
              (is (= "v1" (read-entry view1 "first"))
                  "Historical view should contain first entry")
              (is (nil? (read-entry view1 "second"))
                  "Historical view should NOT contain second entry")))
          ;; as-of snap2 should have both
          (let [view2 (p/as-of sys snap2)]
            (is (some? view2) "as-of snap2 should return a view")
            (when (satisfies? p/SystemIdentity view2)
              (is (= 2 (count-entries view2))
                  "View at snap2 should have 2 entries")
              (is (= "v2" (read-entry view2 "second"))
                  "View at snap2 should contain second entry"))))
        (finally (close! sys))))))

(defn test-delete-entry-consistency
  [{:keys [create-system write-entry read-entry count-entries delete-entry commit close!]}]
  (when delete-entry
    (testing "deleted entry is no longer readable after commit"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys
                        (write-entry "keep" "keep-val")
                        (write-entry "remove" "remove-val")
                        (commit "two entries"))]
            (is (= 2 (count-entries sys)))
            (is (= "remove-val" (read-entry sys "remove")))
            (let [sys (-> sys (delete-entry "remove") (commit "delete one"))]
              (is (= 1 (count-entries sys))
                  "Count should decrease after delete")
              (is (nil? (read-entry sys "remove"))
                  "Deleted entry should not be readable")
              (is (= "keep-val" (read-entry sys "keep"))
                  "Non-deleted entry should still be readable")))
          (finally (close! sys)))))))

(defn test-overwrite-entry
  [{:keys [create-system write-entry read-entry count-entries delete-entry commit close!]}]
  (when delete-entry
    (testing "overwriting an entry updates its value"
      (let [sys (create-system)]
        (try
          (let [sys (-> sys (write-entry "key" "original") (commit "original value"))]
            (is (= "original" (read-entry sys "key")))
            ;; Overwrite: delete + write
            (let [sys (-> sys
                          (delete-entry "key")
                          (write-entry "key" "updated")
                          (commit "updated value"))]
              (is (= "updated" (read-entry sys "key"))
                  "Should read the updated value")
              (is (= 1 (count-entries sys))
                  "Count should remain 1 after overwrite")))
          (finally (close! sys)))))))

(defn test-concurrent-data-integrity
  [{:keys [create-system write-entry count-entries commit close!] :as fix}]
  (when (get fix :supports-concurrent? true)
    (testing "concurrent writes don't corrupt data"
      (let [sys (create-system)
            sys-atom (atom sys)
            n-threads 4
            n-writes 10
            errors (atom [])
            latch (java.util.concurrent.CountDownLatch. n-threads)]
        (try
          ;; Each thread writes to its own keys on main
          (dotimes [t n-threads]
            (.start
             (Thread.
              (fn []
                (try
                  (dotimes [i n-writes]
                    (let [k (str "t" t "-k" i)]
                      (swap! sys-atom (fn [s] (-> s (write-entry k (str "val-" t "-" i)) (commit (str "thread-" t "-write-" i)))))))
                  (catch Throwable e
                    (swap! errors conj {:thread t :error e}))
                  (finally
                    (.countDown latch)))))))
          (.await latch 30 java.util.concurrent.TimeUnit/SECONDS)
          (is (empty? @errors)
              (str "No errors during concurrent data writes: "
                   (mapv #(-> % :error .getMessage) @errors)))
          ;; All entries should be readable
          (is (= (* n-threads n-writes) (count-entries @sys-atom))
              "All entries from all threads should be present")
          (finally (close! @sys-atom)))))))

;; ============================================================
;; Full test suite
;; ============================================================

(def all-tests
  "All compliance test functions, organized by protocol layer."
  {:system-identity [test-system-identity]
   :snapshotable [test-snapshot-id-after-commit
                  test-parent-ids-root-commit
                  test-parent-ids-chain
                  test-snapshot-meta
                  test-as-of]
   :branchable [test-initial-branches
                test-create-branch
                test-checkout
                test-branch-isolation
                test-delete-branch]
   :graphable [test-history
               test-history-limit
               test-ancestors
               test-ancestor-predicate
               test-common-ancestor
               test-commit-graph
               test-commit-info]
   :mergeable [test-merge
               test-merge-parent-ids
               test-conflicts-empty-for-compatible]
   :concurrent [test-concurrent-commits
                test-concurrent-branch-creation
                test-concurrent-readers]
   :data-consistency [test-write-read-roundtrip
                      test-count-after-writes
                      test-multiple-entries-readable
                      test-branch-data-isolation
                      test-merge-data-visibility
                      test-as-of-data-consistency
                      test-delete-entry-consistency
                      test-overwrite-entry
                      test-concurrent-data-integrity]})

(defn run-compliance-tests
  "Run all compliance tests for the given fixture map.

   Required fixture-map keys:
     :create-system  - (fn [] system)
     :mutate         - (fn [system] new-system)
     :commit         - (fn [system msg] new-system)
     :close!         - (fn [system] ...)
     :write-entry    - (fn [system key value] new-system)
     :read-entry     - (fn [system key] value-or-nil)
     :count-entries  - (fn [system] n)
     :delete-entry   - (fn [system key] new-system) or nil for append-only stores

   Optional keys:
     :supports-concurrent? - false to skip concurrent stress tests (default true)

   Tests for optional protocol layers (branchable, graphable, mergeable)
   are skipped automatically based on the system's capabilities."
  [fixture-map]
  (doseq [[_layer tests] all-tests
          test-fn tests]
    (test-fn fixture-map)))
