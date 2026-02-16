(ns yggdrasil.workspace-test
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.workspace :as ws]
            [yggdrasil.hooks :as hooks]
            [yggdrasil.registry :as reg]
            [yggdrasil.types :as t]
            [yggdrasil.protocols :as p])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; ============================================================
;; Mock system for workspace tests
;; ============================================================

(defrecord MockCommittableSystem [id branch-name snapshots-atom]
  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :mock)
  (capabilities [_] (t/->Capabilities true true false false false false false))

  p/Snapshotable
  (snapshot-id [_] (last @snapshots-atom))
  (parent-ids [_]
    (let [snaps @snapshots-atom]
      (if (> (count snaps) 1)
        #{(nth snaps (- (count snaps) 2))}
        #{})))
  (as-of [_ _] nil)
  (as-of [_ _ _] nil)
  (snapshot-meta [_ _] nil)
  (snapshot-meta [_ _ _] nil)

  p/Branchable
  (branches [_] #{(keyword branch-name)})
  (branches [_ _] #{(keyword branch-name)})
  (current-branch [_] (keyword branch-name))
  (branch! [this _] this)
  (branch! [this _ _] this)
  (branch! [this _ _ _] this)
  (delete-branch! [this _] this)
  (delete-branch! [this _ _] this)
  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _]
    (if (= (keyword name) (keyword branch-name))
      this
      (->MockCommittableSystem id (clojure.core/name name) snapshots-atom))))

(defn- make-mock-system [id branch initial-snaps]
  (->MockCommittableSystem id branch (atom (vec initial-snaps))))

(defn- mock-commit!
  "Simulate a commit on a mock system. Returns the new commit-id."
  [system]
  (let [commit-id (str (java.util.UUID/randomUUID))]
    (swap! (:snapshots-atom system) conj commit-id)
    commit-id))

;; ============================================================
;; Workspace lifecycle tests
;; ============================================================

(deftest test-create-workspace
  (testing "create in-memory workspace"
    (let [w (ws/create-workspace)]
      (is (some? w))
      (is (empty? (ws/list-systems w)))
      (ws/close! w))))

(deftest test-add-remove-system
  (testing "add and remove systems from workspace"
    (let [w (ws/create-workspace)
          sys1 (make-mock-system "git:repo1" "main" ["snap-0"])
          sys2 (make-mock-system "zfs:pool1" "main" ["snap-a"])]
      (ws/add-system! w sys1)
      (ws/add-system! w sys2)
      (is (= 2 (count (ws/list-systems w))))
      (is (some? (ws/get-system w "git:repo1")))

      (ws/remove-system! w "git:repo1")
      (is (= 1 (count (ws/list-systems w))))
      (is (nil? (ws/get-system w "git:repo1")))

      (ws/close! w))))

;; ============================================================
;; HLC coordination tests
;; ============================================================

(deftest test-hlc-ticking
  (testing "tick advances HLC monotonically"
    (let [w (ws/create-workspace)
          hlc1 (ws/tick! w)
          hlc2 (ws/tick! w)
          hlc3 (ws/tick! w)]
      (is (neg? (t/hlc-compare hlc1 hlc2)))
      (is (neg? (t/hlc-compare hlc2 hlc3)))
      (ws/close! w))))

(deftest test-begin-transaction
  (testing "begin-transaction returns a pinned HLC"
    (let [w (ws/create-workspace)
          t1 (ws/begin-transaction! w)
          t2 (ws/begin-transaction! w)]
      (is (some? t1))
      (is (neg? (t/hlc-compare t1 t2)))
      (ws/close! w))))

;; ============================================================
;; Coordinated commit tests
;; ============================================================

(deftest test-commit-with-hlc
  (testing "commit-with-hlc registers entry in registry"
    (let [w (ws/create-workspace)
          sys (make-mock-system "git:repo1" "main" ["snap-0"])]
      (ws/add-system! w sys)

      (let [pinned (ws/begin-transaction! w)
            entry (ws/commit-with-hlc! w "git:repo1" pinned mock-commit!)]
        (is (some? entry))
        (is (= "git:repo1" (:system-id entry)))
        (is (= "main" (:branch-name entry)))
        (is (= pinned (:hlc entry)))
        ;; Should be in the registry
        (is (seq (reg/snapshot-refs (:registry w) (:snapshot-id entry)))))

      (ws/close! w))))

(deftest test-coordinated-commit-multi-system
  (testing "coordinated commit pins same HLC across systems"
    (let [w (ws/create-workspace)
          sys1 (make-mock-system "git:repo1" "main" ["snap-0"])
          sys2 (make-mock-system "zfs:pool1" "main" ["snap-a"])]
      (ws/add-system! w sys1)
      (ws/add-system! w sys2)

      (let [result (ws/coordinated-commit! w
                                           {"git:repo1" mock-commit!
                                            "zfs:pool1" mock-commit!})
            git-entry (get (:results result) "git:repo1")
            zfs-entry (get (:results result) "zfs:pool1")]
        ;; No errors
        (is (empty? (:errors result)))
        ;; Both entries should have the same HLC
        (is (zero? (t/hlc-compare (:hlc git-entry) (:hlc zfs-entry))))
        ;; Both should be in the registry
        (is (seq (reg/snapshot-refs (:registry w) (:snapshot-id git-entry))))
        (is (seq (reg/snapshot-refs (:registry w) (:snapshot-id zfs-entry)))))

      (ws/close! w))))

;; ============================================================
;; Branch ref management tests
;; ============================================================

(deftest test-hold-and-release-ref
  (testing "hold-ref registers in registry, release-ref cleans up"
    (let [w (ws/create-workspace)
          sys (make-mock-system "git:repo1" "feature" ["snap-f1"])]
      (ws/hold-ref! w "git:repo1/:feature" sys)

      ;; Should be in held refs
      (is (= 1 (count (ws/held-refs w))))
      ;; Should be in registry
      (is (seq (reg/snapshot-refs (:registry w) "snap-f1")))

      (ws/release-ref! w "git:repo1/:feature")
      (is (empty? (ws/held-refs w)))

      (ws/close! w))))

(deftest test-connection-cache
  (testing "connection cache stores system values for reuse"
    (let [w (ws/create-workspace)
          sys (make-mock-system "git:repo1" "feature" ["snap-f1"])]
      (ws/hold-ref! w "git:repo1/:feature" sys)

      ;; Should be cached
      (is (some? (ws/get-cached-connection w "git:repo1" "feature")))

      ;; Release clears cache
      (ws/release-ref! w "git:repo1/:feature")
      (is (nil? (ws/get-cached-connection w "git:repo1" "feature")))

      (ws/close! w))))

;; ============================================================
;; Temporal query tests
;; ============================================================

(deftest test-as-of-world
  (testing "as-of-world returns cross-system state at time T"
    (let [w (ws/create-workspace)
          sys1 (make-mock-system "git:repo1" "main" ["snap-0"])
          sys2 (make-mock-system "zfs:pool1" "main" ["snap-a"])]
      (ws/add-system! w sys1)
      (ws/add-system! w sys2)

      ;; Record initial state HLC
      (let [t0 (ws/current-hlc w)]
        ;; Do a coordinated commit
        (let [result (ws/coordinated-commit! w
                                             {"git:repo1" mock-commit!
                                              "zfs:pool1" mock-commit!})
              t1 (:hlc result)]
          ;; Query at t0 — should see initial snapshots
          (let [world-t0 (ws/as-of-world w t0)]
            (is (= "snap-0" (:snapshot-id (get world-t0 ["git:repo1" "main"]))))
            (is (= "snap-a" (:snapshot-id (get world-t0 ["zfs:pool1" "main"])))))

          ;; Query at t1 — should see new snapshots
          (let [world-t1 (ws/as-of-world w t1)]
            (is (not= "snap-0" (:snapshot-id (get world-t1 ["git:repo1" "main"]))))
            (is (not= "snap-a" (:snapshot-id (get world-t1 ["zfs:pool1" "main"])))))))

      (ws/close! w))))

;; ============================================================
;; Error handling tests
;; ============================================================

(deftest test-commit-unknown-system
  (testing "commit-with-hlc throws for unknown system"
    (let [w (ws/create-workspace)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"System not found"
                            (ws/commit-with-hlc! w "nonexistent"
                                                  (t/hlc-now)
                                                  identity)))
      (ws/close! w))))

;; ============================================================
;; Multi-branch workspace test
;; ============================================================

(deftest test-multi-branch-workspace
  (testing "hold refs to multiple branches across systems"
    (let [w (ws/create-workspace)
          git-main (make-mock-system "git:repo1" "main" ["g1"])
          git-feat (make-mock-system "git:repo1" "feature" ["g2"])
          zfs-main (make-mock-system "zfs:pool1" "main" ["z1"])]
      (ws/add-system! w git-main)
      (ws/hold-ref! w "git:repo1/:main" git-main)
      (ws/hold-ref! w "git:repo1/:feature" git-feat)
      (ws/hold-ref! w "zfs:pool1/:main" zfs-main)

      ;; Should have 3 held refs
      (is (= 3 (count (ws/held-refs w))))

      ;; All should be in registry
      (is (seq (reg/snapshot-refs (:registry w) "g1")))
      (is (seq (reg/snapshot-refs (:registry w) "g2")))
      (is (seq (reg/snapshot-refs (:registry w) "z1")))

      ;; Release one
      (ws/release-ref! w "git:repo1/:feature")
      (is (= 2 (count (ws/held-refs w))))

      (ws/close! w))))

;; ============================================================
;; Managed system tests
;; ============================================================

(deftest test-manage-adds-system
  (testing "manage! adds system and registers current snapshot"
    (let [w (ws/create-workspace)
          sys (make-mock-system "git:repo1" "main" ["snap-0"])]
      (ws/manage! w sys)
      (is (= 1 (count (ws/list-systems w))))
      (is (some? (ws/get-system w "git:repo1")))
      ;; Current snapshot should be in registry
      (is (seq (reg/snapshot-refs (:registry w) "snap-0")))
      (ws/close! w))))

(deftest test-unmanage-removes-system
  (testing "unmanage! removes system and cleans up"
    (let [w (ws/create-workspace)
          sys (make-mock-system "git:repo1" "main" ["snap-0"])]
      (ws/manage! w sys)
      (is (= 1 (count (ws/list-systems w))))

      (ws/unmanage! w "git:repo1")
      (is (empty? (ws/list-systems w)))
      (is (nil? (ws/get-system w "git:repo1")))
      (ws/close! w))))

;; ============================================================
;; Partial failure tests
;; ============================================================

(deftest test-coordinated-commit-partial-failure
  (testing "coordinated-commit! captures per-system errors"
    (let [w (ws/create-workspace)
          sys1 (make-mock-system "git:repo1" "main" ["snap-0"])
          sys2 (make-mock-system "zfs:pool1" "main" ["snap-a"])]
      (ws/add-system! w sys1)
      (ws/add-system! w sys2)

      (let [result (ws/coordinated-commit! w
                                           {"git:repo1" mock-commit!
                                            "zfs:pool1" (fn [_] (throw (ex-info "ZFS pool offline" {})))})]
        ;; Git should succeed
        (is (some? (get (:results result) "git:repo1")))
        ;; ZFS should fail
        (is (some? (get (:errors result) "zfs:pool1")))
        ;; HLC should be returned
        (is (some? (:hlc result)))
        ;; Git entry should be in registry
        (is (seq (reg/snapshot-refs (:registry w)
                                   (:snapshot-id (get (:results result) "git:repo1"))))))

      (ws/close! w))))

(deftest test-coordinated-commit-all-fail
  (testing "coordinated-commit! handles all systems failing"
    (let [w (ws/create-workspace)
          sys1 (make-mock-system "git:repo1" "main" ["snap-0"])]
      (ws/add-system! w sys1)

      (let [result (ws/coordinated-commit! w
                                           {"git:repo1" (fn [_] (throw (ex-info "disk full" {})))})]
        (is (empty? (:results result)))
        (is (= 1 (count (:errors result))))
        (is (some? (:hlc result))))

      (ws/close! w))))

;; ============================================================
;; Watchable mock for manage! integration
;; ============================================================

(defrecord MockWatchableSystem [id branch-name snapshots-atom watchers-atom]
  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :mock-watchable)
  (capabilities [_] (t/->Capabilities true true false false false true false))

  p/Snapshotable
  (snapshot-id [_] (last @snapshots-atom))
  (parent-ids [_]
    (let [snaps @snapshots-atom]
      (if (> (count snaps) 1)
        #{(nth snaps (- (count snaps) 2))}
        #{})))
  (as-of [_ _] nil)
  (as-of [_ _ _] nil)
  (snapshot-meta [_ _] nil)
  (snapshot-meta [_ _ _] nil)

  p/Branchable
  (branches [_] #{(keyword branch-name)})
  (branches [_ _] #{(keyword branch-name)})
  (current-branch [_] (keyword branch-name))
  (branch! [this _] this)
  (branch! [this _ _] this)
  (branch! [this _ _ _] this)
  (delete-branch! [this _] this)
  (delete-branch! [this _ _] this)
  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _]
    (if (= (keyword name) (keyword branch-name))
      this
      (->MockWatchableSystem id (clojure.core/name name)
                             snapshots-atom watchers-atom)))

  p/Watchable
  (watch! [this callback] (p/watch! this callback {}))
  (watch! [_ callback _opts]
    (let [watch-id (str "watch-" (java.util.UUID/randomUUID))]
      (swap! watchers-atom assoc watch-id callback)
      watch-id))
  (unwatch! [this watch-id] (p/unwatch! this watch-id nil))
  (unwatch! [_ watch-id _opts]
    (swap! watchers-atom dissoc watch-id)))

(defn- make-watchable-system [id branch initial-snaps]
  (->MockWatchableSystem id branch (atom (vec initial-snaps)) (atom {})))

(defn- simulate-commit!
  "Simulate an external commit, firing all registered watchers."
  [sys]
  (let [commit-id (str (java.util.UUID/randomUUID))]
    (swap! (:snapshots-atom sys) conj commit-id)
    (doseq [[_ cb] @(:watchers-atom sys)]
      (cb {:type :commit
           :snapshot-id commit-id
           :branch (name (:branch-name sys))
           :timestamp (System/currentTimeMillis)}))
    commit-id))

;; ============================================================
;; manage! integration with Watchable
;; ============================================================

(deftest test-manage-watchable-auto-registers
  (testing "manage! with Watchable system auto-registers commits"
    (let [w (ws/create-workspace)
          sys (make-watchable-system "mock:w1" "main" ["snap-0"])]
      (ws/manage! w sys)

      ;; Initial snapshot registered
      (is (seq (reg/snapshot-refs (:registry w) "snap-0")))

      ;; Simulate external commit — should auto-register via Watchable hook
      (let [new-id (simulate-commit! sys)]
        ;; Give the callback time to fire (it's synchronous in our mock)
        (is (seq (reg/snapshot-refs (:registry w) new-id))
            "Commit made after manage! should be auto-registered"))

      ;; Simulate second commit
      (let [new-id2 (simulate-commit! sys)]
        (is (seq (reg/snapshot-refs (:registry w) new-id2))))

      ;; Unmanage should clean up watchers
      (ws/unmanage! w "mock:w1")
      (is (empty? @(:watchers-atom sys))
          "Watchable watchers should be cleaned up after unmanage!")

      (ws/close! w))))

;; ============================================================
;; Persistent workspace tests
;; ============================================================

(defn- temp-dir []
  (str (Files/createTempDirectory "yggdrasil-ws-test-"
                                   (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(deftest test-workspace-persistence
  (testing "workspace with :store-path persists registry across close/reopen"
    (let [store-path (temp-dir)]
      (try
        ;; Create workspace with persistence, add system, commit
        (let [w (ws/create-workspace {:store-path store-path})
              sys (make-mock-system "git:repo1" "main" ["snap-0"])]
          (ws/add-system! w sys)
          (ws/commit-with-hlc! w "git:repo1"
                               (ws/begin-transaction! w)
                               mock-commit!)
          ;; Verify entry count before close
          (is (= 2 (reg/entry-count (:registry w)))
              "Should have 2 entries (add-system + commit)")
          (ws/close! w))

        ;; Reopen workspace from same store path
        (let [w2 (ws/create-workspace {:store-path store-path})]
          (is (= 2 (reg/entry-count (:registry w2)))
              "Persisted registry should have 2 entries after reopen")
          ;; snap-0 should still be findable
          (is (seq (reg/snapshot-refs (:registry w2) "snap-0"))
              "Original snapshot should be in persisted registry")
          (ws/close! w2))
        (finally
          (delete-dir-recursive store-path))))))
