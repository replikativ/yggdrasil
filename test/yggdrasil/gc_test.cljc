(ns yggdrasil.gc-test
  "GC coordinator tests. PORTABLE: the reachability / candidate / sweep / report
   tests run on a MockSystem defrecord and exercise the .cljc coordinator on BOTH
   the JVM and cljs/node. The git end-to-end reclamation test and the
   java.util.Date / `requiring-resolve` opts-forwarding tests are JVM-only (a real
   git repo + Date sentinels) and live under #?(:clj …)."
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.gc :as gc]
            [yggdrasil.registry :as reg]
            [yggdrasil.types :as t]
            [yggdrasil.protocols :as p]
            #?@(:clj [[yggdrasil.adapters.git :as git]
                      [clojure.java.shell :refer [sh]]
                      [clojure.string :as str]]))
  #?(:clj (:import [java.nio.file Files]
                   [java.nio.file.attribute FileAttribute])))

;; ============================================================
;; Mock system for testing
;; ============================================================

(defn- index-of
  "Portable first-index-of (replaces JVM `.indexOf`); -1 when absent."
  [coll x]
  (or (first (keep-indexed (fn [i v] (when (= v x) i)) coll)) -1))

(defrecord MockSystem [id branch-name snapshots swept-atom]
  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :mock)
  ;; 9 booleans: snapshotable branchable graphable mergeable overlayable
  ;; watchable garbage-collectable addressable committable
  (capabilities [_] (t/->Capabilities true true true false false false true false false))

  p/Snapshotable
  (snapshot-id [_] (last snapshots))
  (parent-ids [_]
    (if (> (count snapshots) 1)
      #{(nth snapshots (- (count snapshots) 2))}
      #{}))
  (as-of [_ _ _] nil)
  (as-of [_ _] nil)
  (snapshot-meta [_ _ _] nil)
  (snapshot-meta [_ _] nil)

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
      (throw (ex-info "Branch not found" {:branch name}))))

  p/Graphable
  (history [this] (p/history this nil))
  (history [_ _] (vec (reverse snapshots)))
  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _]
    (let [idx (index-of (vec snapshots) (str snap-id))]
      (if (pos? idx)
        (vec (take idx snapshots))
        [])))
  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _]
    (let [ids (vec snapshots)
          idx-a (index-of ids (str a))
          idx-b (index-of ids (str b))]
      (and (>= idx-a 0) (>= idx-b 0) (< idx-a idx-b))))
  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ _ _ _] nil)
  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _] {:nodes {} :branches {} :roots #{}})
  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [_ _ _] nil)

  p/GarbageCollectable
  (gc-roots [_] (if (last snapshots) #{(last snapshots)} #{}))
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids _]
    (when swept-atom
      (swap! swept-atom into snapshot-ids))
    this))

(defn- make-mock-system
  [id branch snapshots]
  (->MockSystem id branch snapshots (atom #{})))

(defn- make-entry [snap-id system-id branch ts]
  (t/->RegistryEntry snap-id system-id branch (t/->HLC ts 0) nil nil nil))

;; ============================================================
;; Reachability tests
;; ============================================================

(deftest test-compute-reachable-set
  (testing "reachable set includes branch heads and ancestors"
    (let [sys (make-mock-system "git:repo1" "main"
                                ["snap-1" "snap-2" "snap-3"])
          reachable (gc/compute-reachable-set [sys])]
      ;; Head (snap-3) and all ancestors should be reachable
      (is (contains? reachable "snap-3"))
      (is (contains? reachable "snap-2"))
      (is (contains? reachable "snap-1")))))

(deftest test-compute-gc-roots
  (testing "gc-roots returns branch heads from GarbageCollectable systems"
    (let [sys (make-mock-system "git:repo1" "main"
                                ["snap-1" "snap-2" "snap-3"])
          roots (gc/compute-gc-roots [sys])]
      (is (contains? roots "snap-3")))))

;; ============================================================
;; JVM-only from here down. These tests drive the durable 2P-Set REGISTRY
;; (reg/create-registry), whose konserve ops are async on cljs (CPS, not values),
;; plus a real git repo and java.util.Date sentinels. Porting the registry tests
;; to cljs needs the deftest-async/<? harness (a follow-up); the pure reachability
;; tests above already run cross-platform.
;; ============================================================

#?(:clj
   (do

;; ============================================================
;; GC candidate selection tests
;; ============================================================

(deftest test-gc-candidates-excludes-reachable
  (testing "reachable snapshots are never GC candidates"
    (let [r (reg/create-registry)
          ;; Register entries, some old enough for GC
          old-time (- (t/now-ms) (* 8 24 60 60 1000)) ; 8 days ago
          e1 (make-entry "snap-1" "git:repo1" "main" old-time)
          e2 (make-entry "snap-2" "git:repo1" "main" (+ old-time 1000))
          e3 (make-entry "snap-3" "git:repo1" "main" (t/now-ms))]
      (reg/register-batch! r [e1 e2 e3])

      ;; snap-3 is the head (reachable), snap-1 and snap-2 are old
      (let [reachable #{"snap-2" "snap-3"}
            candidates (gc/gc-candidates r reachable
                                         (* 7 24 60 60 1000))]
        ;; Only snap-1 should be a candidate (old + unreachable)
        (is (= 1 (count candidates)))
        (is (= "snap-1" (:snapshot-id (first candidates)))))

      (reg/close! r))))

(deftest test-gc-candidates-respects-grace-period
  (testing "recent snapshots are not GC candidates even if unreachable"
    (let [r (reg/create-registry)
          now (t/now-ms)
          ;; Recent entry (1 hour ago) — within 7-day grace period
          e1 (make-entry "snap-1" "git:repo1" "main" (- now 3600000))]
      (reg/register! r e1)

      (let [candidates (gc/gc-candidates r #{} (* 7 24 60 60 1000))]
        (is (empty? candidates)))

      (reg/close! r))))

;; ============================================================
;; GC sweep tests
;; ============================================================

(deftest test-gc-sweep-delegates-to-adapter
  (testing "gc-sweep! calls GarbageCollectable.gc-sweep! on the adapter"
    (let [r (reg/create-registry)
          swept (atom #{})
          sys (->MockSystem "git:repo1" "main"
                            ["snap-3"] ; only head is snap-3
                            swept)
          old-time (- (t/now-ms) (* 8 24 60 60 1000))
          e1 (make-entry "snap-1" "git:repo1" "main" old-time)
          e2 (make-entry "snap-2" "git:repo1" "main" (+ old-time 1000))
          e3 (make-entry "snap-3" "git:repo1" "main" (t/now-ms))]
      (reg/register-batch! r [e1 e2 e3])

      (let [result (gc/gc-sweep! r [sys] {:grace-period-ms (* 7 24 60 60 1000)})]
        ;; snap-1 and snap-2 should be swept (old + unreachable)
        (is (= #{"snap-1" "snap-2"} @swept))
        ;; They should be removed from the registry
        (is (nil? (reg/snapshot-refs r "snap-1")))
        (is (nil? (reg/snapshot-refs r "snap-2")))
        ;; snap-3 should remain
        (is (seq (reg/snapshot-refs r "snap-3")))
        ;; No errors
        (is (empty? (:errors result))))

      (reg/close! r))))

(deftest test-gc-sweep-dry-run
  (testing "dry-run reports candidates without deleting"
    (let [r (reg/create-registry)
          sys (make-mock-system "git:repo1" "main" ["snap-3"])
          old-time (- (t/now-ms) (* 8 24 60 60 1000))
          e1 (make-entry "snap-1" "git:repo1" "main" old-time)]
      (reg/register! r e1)
      (reg/register! r (make-entry "snap-3" "git:repo1" "main"
                                   (t/now-ms)))

      (let [result (gc/gc-sweep! r [sys] {:dry-run? true})]
        ;; Candidates reported but not deleted
        (is (seq (:candidates result)))
        ;; Entry still in registry
        (is (seq (reg/snapshot-refs r "snap-1"))))

      (reg/close! r))))

;; ============================================================
;; GC report tests
;; ============================================================

(deftest test-gc-report
  (testing "gc-report shows eligible entries grouped by system"
    (let [r (reg/create-registry)
          sys1 (make-mock-system "git:repo1" "main" ["snap-3"])
          sys2 (make-mock-system "zfs:pool1" "main" ["snap-z"])
          old-time (- (t/now-ms) (* 8 24 60 60 1000))]
      (reg/register-batch! r
                           [(make-entry "snap-1" "git:repo1" "main" old-time)
                            (make-entry "snap-2" "git:repo1" "main" (+ old-time 1000))
                            (make-entry "snap-3" "git:repo1" "main" (t/now-ms))
                            (make-entry "snap-x" "zfs:pool1" "main" old-time)
                            (make-entry "snap-z" "zfs:pool1" "main" (t/now-ms))])

      (let [report (gc/gc-report r [sys1 sys2])]
        (is (= 5 (:total-entries report)))
        ;; 3 candidates: snap-1, snap-2, snap-x
        (is (= 3 (:gc-eligible report)))
        (is (= #{"git:repo1" "zfs:pool1"}
               (set (keys (:by-system report))))))

      (reg/close! r))))

;; ============================================================
;; Cross-system GC safety test
;; ============================================================

(deftest test-gc-cross-system-safety
  (testing "snapshot referenced by another system is not collected"
    (let [r (reg/create-registry)
          ;; snap-1 exists in both git and btrfs
          old-time (- (t/now-ms) (* 8 24 60 60 1000))
          swept-git (atom #{})
          ;; Git only has snap-3 as head (snap-1 is old and unreachable via git)
          git-sys (->MockSystem "git:repo1" "main" ["snap-3"] swept-git)
          ;; Btrfs has snap-1 as its head (snap-1 is reachable via btrfs)
          btrfs-sys (make-mock-system "btrfs:vol1" "main" ["snap-1"])]

      (reg/register-batch! r
                           [(make-entry "snap-1" "git:repo1" "main" old-time)
                            (make-entry "snap-1" "btrfs:vol1" "main" old-time)
                            (make-entry "snap-3" "git:repo1" "main" (t/now-ms))])

      ;; Compute reachable from BOTH systems
      (let [reachable (gc/compute-reachable-set [git-sys btrfs-sys])]
        ;; snap-1 should be reachable because btrfs has it as head
        (is (contains? reachable "snap-1"))
        ;; Therefore it should NOT be a candidate
        (let [candidates (gc/gc-candidates r reachable (* 7 24 60 60 1000))]
          (is (not (some #(= "snap-1" (:snapshot-id %)) candidates)))))

      (reg/close! r))))

;; ============================================================
;; End-to-end GC reclamation test with real Git
;; ============================================================

(defn- temp-dir []
  (str (Files/createTempDirectory "yggdrasil-gc-e2e-"
                                  (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(defn- git-cmd [dir & args]
  (let [result (apply sh "git" "-C" dir args)]
    (when-not (zero? (:exit result))
      (throw (ex-info "git failed" {:err (:err result) :args (vec args)})))
    (str/trim (:out result))))

(defn- git-object-exists? [dir sha]
  (zero? (:exit (sh "git" "-C" dir "cat-file" "-t" sha))))

(deftest ^:e2e test-gc-reclamation-git
  (testing "GC coordinator actually reclaims unreachable git objects"
    (let [path (temp-dir)]
      (try
        ;; 1. Create a git repo with an initial commit on main
        (let [sys (git/init! path {:system-name "gc-test-git"})
              _ (spit (str path "/entries/base") "base content")
              _ (git-cmd path "add" "-A")
              _ (git-cmd path "commit" "-m" "initial commit")
              main-sha (git-cmd path "rev-parse" "HEAD")

              ;; 2. Create a feature branch with exclusive commits
              sys-with-branch (p/branch! sys :feature)
              feature-wt (str (:worktrees-dir sys) "/feature")
              _ (spit (str feature-wt "/entries/feature-file") "feature content")
              _ (git-cmd feature-wt "add" "-A")
              _ (git-cmd feature-wt "commit" "-m" "feature commit 1")
              feature-sha1 (git-cmd feature-wt "rev-parse" "HEAD")
              _ (spit (str feature-wt "/entries/feature-file2") "more content")
              _ (git-cmd feature-wt "add" "-A")
              _ (git-cmd feature-wt "commit" "-m" "feature commit 2")
              feature-sha2 (git-cmd feature-wt "rev-parse" "HEAD")]

          ;; 3. Verify feature commits exist
          (is (git-object-exists? path feature-sha1)
              "Feature commit 1 should exist before GC")
          (is (git-object-exists? path feature-sha2)
              "Feature commit 2 should exist before GC")

          ;; 4. Delete the feature branch (makes commits unreachable)
          (p/delete-branch! sys :feature)

          ;; 5. Register entries in the registry with old timestamps
          ;;    so they're eligible for GC
          (let [r (reg/create-registry)
                old-time (- (t/now-ms) (* 8 24 60 60 1000))
                e-main (make-entry main-sha "gc-test-git" "main"
                                   (t/now-ms))
                e-feat1 (make-entry feature-sha1 "gc-test-git" "feature"
                                    old-time)
                e-feat2 (make-entry feature-sha2 "gc-test-git" "feature"
                                    (+ old-time 1000))]
            (reg/register-batch! r [e-main e-feat1 e-feat2])

            ;; 6. Run the GC coordinator. grace 0 ⇒ expire reflog + prune "now" so
            ;; the just-unreachable feature objects are reclaimed immediately (the
            ;; conservative default keeps git's 2-week grace, retaining fresh objects).
            (let [result (gc/gc-sweep! r [sys] {:grace-period-ms 0})]
              ;; Feature entries should be swept from registry
              (is (= 2 (count (:swept result)))
                  "Two feature entries should be swept")
              (is (empty? (:errors result))
                  "No errors during GC")

              ;; Main entry should survive
              (is (seq (reg/snapshot-refs r main-sha))
                  "Main entry should remain in registry")

              ;; Feature entries should be gone from registry
              (is (nil? (reg/snapshot-refs r feature-sha1))
                  "Feature commit 1 should be removed from registry")
              (is (nil? (reg/snapshot-refs r feature-sha2))
                  "Feature commit 2 should be removed from registry"))

            ;; 7. Verify git objects are actually gone
            (is (not (git-object-exists? path feature-sha1))
                "Feature commit 1 object should be reclaimed from git")
            (is (not (git-object-exists? path feature-sha2))
                "Feature commit 2 object should be reclaimed from git")

            ;; 8. Main commit should still exist
            (is (git-object-exists? path main-sha)
                "Main commit should survive GC")

            (reg/close! r)))
        (finally
          (delete-dir-recursive path)
          (delete-dir-recursive (str path "-worktrees")))))))

;; ============================================================
;; Opts threading + report contract (regression for the GC-into-shape pass)
;; ============================================================

;; A GarbageCollectable that RECORDS the (snapshot-ids, opts) it was handed and
;; returns a reclamation REPORT (the new contract), so we can assert opts reach it.
(defrecord OptsRecorderSystem [id seen]
  p/SystemIdentity (system-id [_] id) (system-type [_] :recorder)
  p/Snapshotable
  (snapshot-id [_] (str "snap-" id))
  (snapshot-meta [_ _] nil)
  p/GarbageCollectable
  (gc-roots [_] #{})
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [_ snapshot-ids opts]
    (reset! seen {:snapshot-ids snapshot-ids :opts opts})
    {:system-id id :reclaimed 7}))

(deftest test-composite-forwards-opts-and-returns-reports
  (testing "CompositeSystem.gc-sweep! forwards opts to each leaf and returns {sid → report}"
    (let [a (->OptsRecorderSystem "a" (atom nil))
          b (->OptsRecorderSystem "b" (atom nil))
          comp ((requiring-resolve 'yggdrasil.composite/composite) [a b])
          opts {:remove-before (java.util.Date. 123) :dry-run? false}
          report (p/gc-sweep! comp #{"snap-x"} opts)]
      (is (= opts (:opts @(:seen a))) "leaf a received the forwarded opts")
      (is (= opts (:opts @(:seen b))) "leaf b received the forwarded opts")
      (is (= {:system-id "a" :reclaimed 7} (get report "a")) "report keyed by system-id")
      (is (= {:system-id "b" :reclaimed 7} (get report "b"))))))

(deftest test-coordinator-forwards-opts-to-adapter
  (testing "yggdrasil.gc/gc-sweep! forwards opts down to the adapter"
    (let [sys (->OptsRecorderSystem "git:r" (atom nil))
          r   (reg/create-registry)
          old (- (t/now-ms) (* 30 24 60 60 1000))]
      (reg/register! r (make-entry "snap-old" "git:r" "main" old))
      (gc/gc-sweep! r [sys] {:grace-period-ms (* 7 24 60 60 1000)
                             :remove-before (java.util.Date. 99)})
      (is (= (java.util.Date. 99) (:remove-before (:opts @(:seen sys))))
          "adapter saw remove-before from the coordinator's opts"))))

(deftest test-gc-system-convenience
  (testing "yggdrasil.gc/gc-system! GCs one system (nil snapshot-ids) and returns its report"
    (let [sys (->OptsRecorderSystem "s" (atom nil))
          report (gc/gc-system! sys {:remove-before (java.util.Date. 5)})]
      (is (= {:system-id "s" :reclaimed 7} report))
      (is (nil? (:snapshot-ids @(:seen sys))) "single-system GC passes nil snapshot-ids")
      (is (= (java.util.Date. 5) (:remove-before (:opts @(:seen sys))))))))

)) ; end #?(:clj (do …)) — registry/git/Date tests are JVM-only (see header above)
