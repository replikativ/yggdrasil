(ns yggdrasil.adapters.ipfs-test
  "Compliance tests for the IPFS adapter.

   Tests require IPFS daemon to be running.
   Start with: ipfs daemon

   WARNING: These tests are SLOW due to IPNS publish times (2-5 min per commit).
   IPNS is necessary for P2P branch propagation, so slowness is expected.

   To run: clj -M:test -n yggdrasil.adapters.ipfs-test

   Set IPFS_FAST_TESTS=true to skip IPNS operations (local-only testing)"
  (:require [clojure.test :refer [deftest testing use-fixtures is]]
            [yggdrasil.adapters.ipfs :as ipfs]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]))

;; ============================================================
;; IPFS daemon check
;; ============================================================

(defn- ipfs-available? []
  (try
    (let [result (sh "ipfs" "id")]
      (zero? (:exit result)))
    (catch Exception _ false)))

(defn ipfs-fixture [f]
  (if (ipfs-available?)
    (do
      (println "IPFS daemon is available, running tests...")
      (f))
    (do
      (println "SKIP: IPFS daemon not running")
      (println "Start with: ipfs daemon"))))

(use-fixtures :once ipfs-fixture)

;; ============================================================
;; Test fixture for compliance suite
;; ============================================================

(defn- unique-system-name []
  (str "test_" (System/nanoTime)))

(defn- add-to-ipfs
  "Add data to IPFS and return CID."
  [data]
  (let [result (sh "ipfs" "add" "-Q" :in data)]
    (when-not (zero? (:exit result))
      (throw (ex-info "Failed to add to IPFS" {:stderr (:err result)})))
    (str/trim (:out result))))

(defn- make-fixture []
  (let [system-name (unique-system-name)
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
                               (let [branches (p/branches sys)]
                                 (doseq [b branches]
                                   (when-not (contains? @known-branches b)
                                     ;; New branch - use last committed state of main as base
                                     (swap! known-branches conj b)
                                     (swap! branch-base assoc b (get @committed-state :main {}))))))
        ;; Detect if a merge happened and import merged data
        detect-merge! (fn [sys]
                        (let [branch (p/current-branch sys)
                              parents (p/parent-ids sys)
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
                      (try
                        (ipfs/init! {:system-name system-name})
                        (catch Exception e
                          (println "Failed to create system:" (.getMessage e))
                          (throw e))))
     :mutate (fn [sys]
               (detect-new-branches! sys)
               ;; Simulate mutation by incrementing counter
               (assoc sys :_mutation-count
                      (inc (or (:_mutation-count sys) 0))))
     :commit (fn [sys msg]
               (detect-new-branches! sys)
               (let [branch (p/current-branch sys)
                     own-entries (get @entries-store branch {})
                     base-entries (get @branch-base branch {})]
                 ;; Save committed state for this branch
                 (swap! committed-state assoc branch (merge base-entries own-entries)))
               ;; Create dummy data and commit
               (let [data (str "data-" (:_mutation-count sys 0))
                     root-cid (add-to-ipfs data)]
                 (p/commit! sys msg {:root root-cid})))
     :close! (fn [sys]
               (try
                 ;; Don't delete keys - they're shared across tests
                 ;; Each test uses unique system names, so state files don't conflict
                 (ipfs/destroy! sys {:delete-state? true
                                     :delete-keys? false})
                 (catch Exception e
                   (println "Warning: cleanup failed:" (.getMessage e)))))
     ;; Note: IPFS adapter doesn't manage user data directly
     ;; Data operations use a branch-aware mock for compliance testing
     ;; Real IPFS workflows: user manages data with `ipfs add`, Yggdrasil tracks versions
     :write-entry (fn [sys key value]
                    (detect-new-branches! sys)
                    (let [branch (p/current-branch sys)]
                      (swap! entries-store assoc-in [branch key] value)
                      sys))
     :read-entry (fn [sys key]
                   (detect-new-branches! sys)
                   (detect-merge! sys)
                   (let [branch (p/current-branch sys)
                         own (get-in @entries-store [branch key])
                         base (get-in @branch-base [branch key])]
                     (or own base)))
     :count-entries (fn [sys]
                      (detect-new-branches! sys)
                      (detect-merge! sys)
                      (let [branch (p/current-branch sys)
                            own-entries (get @entries-store branch {})
                            base-entries (get @branch-base branch {})]
                        (count (merge base-entries own-entries))))
     :delete-entry (fn [sys key]
                     (detect-new-branches! sys)
                     (let [branch (p/current-branch sys)]
                       (swap! entries-store update branch dissoc key)
                       sys))
     :supports-concurrent? false}))

;; ============================================================
;; Compliance tests
;; ============================================================

(deftest ^:ipfs full-compliance-suite
  (when (ipfs-available?)
    (testing "ipfs adapter passes yggdrasil compliance suite"
      (compliance/run-compliance-tests (make-fixture)))))

(deftest ^:ipfs system-identity-tests
  (when (ipfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-system-identity fix))))

(deftest ^:ipfs snapshotable-tests
  (when (ipfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-snapshot-id-after-commit fix)
      (compliance/test-parent-ids-root-commit fix)
      (compliance/test-parent-ids-chain fix)
      (compliance/test-snapshot-meta fix))))

(deftest ^:ipfs branchable-tests
  (when (ipfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-initial-branches fix)
      (compliance/test-create-branch fix)
      (compliance/test-checkout fix)
      (compliance/test-delete-branch fix))))

(deftest ^:ipfs graphable-tests
  (when (ipfs-available?)
    (let [fix (make-fixture)]
      (compliance/test-history fix)
      (compliance/test-history-limit fix)
      (compliance/test-ancestors fix)
      (compliance/test-ancestor-predicate fix))))

(deftest ^:ipfs basic-operations-test
  (when (ipfs-available?)
    (testing "basic ipfs operations"
      (let [system-name (unique-system-name)
            sys (ipfs/init! {:system-name system-name})]
        (try
          ;; Test system identity
          (is (= :ipfs (p/system-type sys)))
          (is (= system-name (p/system-id sys)))

          ;; Create initial commit
          (let [data "test data"
                root-cid (add-to-ipfs data)]
            (ipfs/commit! sys "Initial commit" {:root root-cid})

            ;; Test snapshot exists
            (let [snap-id (p/snapshot-id sys)]
              (is (string? snap-id))
              (is (not (str/blank? snap-id))))

            ;; Test snapshot metadata
            (let [snap-id (p/snapshot-id sys)
                  meta (p/snapshot-meta sys snap-id)]
              (is (= snap-id (:snapshot-id meta)))
              (is (= root-cid (:root meta)))
              (is (= "Initial commit" (:message meta)))))

          ;; Test branching
          (p/branch! sys :test-branch)
          (let [branches (p/branches sys)]
            (is (contains? branches :test-branch)))

          ;; Test checkout
          (let [sys2 (p/checkout sys :test-branch)]
            (is (= :test-branch (p/current-branch sys2))))

          ;; Test delete branch (need to checkout another branch first)
          (p/checkout sys :main)
          (p/delete-branch! sys :test-branch)
          (let [branches (p/branches sys)]
            (is (not (contains? branches :test-branch))))

          (finally
            (ipfs/destroy! sys {:delete-state? true :delete-keys? true})))))))

(deftest ^:ipfs merge-operations-test
  (when (ipfs-available?)
    (testing "ipfs merge operations"
      (let [system-name (unique-system-name)
            sys (ipfs/init! {:system-name system-name})]
        (try
          ;; Create base commit
          (let [data1 "base data"
                root1 (add-to-ipfs data1)]
            (ipfs/commit! sys "Base commit" {:root root1}))

          ;; Create feature branch
          (p/branch! sys :feature)

          ;; Commit to main
          (let [data2 "main data"
                root2 (add-to-ipfs data2)
                sys (ipfs/commit! sys "Main work" {:root root2})]

            ;; Commit to feature
            (let [sys (p/checkout sys :feature)
                  data3 "feature data"
                  root3 (add-to-ipfs data3)
                  sys (ipfs/commit! sys "Feature work" {:root root3})]

              ;; Merge feature into main
              (let [sys (p/checkout sys :main)
                    data-merged "merged data"
                    root-merged (add-to-ipfs data-merged)
                    sys (p/merge! sys :feature {:root root-merged
                                                :message "Merge feature"})]

                ;; Verify merge commit has two parents
                (let [parents (p/parent-ids sys)]
                  (is (= 2 (count parents)))))))

          (finally
            (ipfs/destroy! sys {:delete-state? true :delete-keys? true})))))))

;; ============================================================
;; Manual cleanup
;; ============================================================

(comment
  ;; Check IPFS daemon
  (ipfs-available?)

  ;; Test basic IPFS operations
  (sh "ipfs" "add" "-Q" :in "test data")

  ;; Manual test
  (let [sys (ipfs/init! {:system-name "manual-test"})]
    (try
      (let [root (add-to-ipfs "test data")]
        (ipfs/commit! sys "Test commit" {:root root}))
      (println "Snapshot ID:" (p/snapshot-id sys))
      (p/branch! sys :test)
      (println "Branches:" (p/branches sys))
      (finally
        (ipfs/destroy! sys {:delete-state? true :delete-keys? true})))))
