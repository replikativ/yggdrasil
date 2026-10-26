(ns yggdrasil.adapters.geschichte-test
  "Behaviour of the Geschichte adapter on the JVM.

   `Mergeable` is the reason this adapter moved into yggdrasil: while it lived
   in geschichte it implemented SystemIdentity/Snapshotable/Branchable/Graphable
   but NOT Mergeable, so a Geschichte-backed system had no `diff` at all and
   consumers fell back to a bare datom count of geschichte's INTERNAL
   commit/tree/blob schema."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.versioning :as versioning]
            [datahike.index.audit :as audit]
            [geschichte.bytes :as gbytes]
            [geschichte.query :as query]
            [geschichte.repo :as repo]
            [geschichte.workspace :as workspace]
            [yggdrasil.adapters.geschichte :as gy]
            [yggdrasil.adapters.cleanup-test-support :as cleanup-test]
            [yggdrasil.adapters.datahike-cleanup :as cleanup-owner]
            [yggdrasil.convergent.overlay :as overlay]
            [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]))

(defn- ->bytes [s] (gbytes/utf8 s))
(defn- text [bs] (when bs (gbytes/decode-utf8 bs)))

(defn- with-repo
  "Run `f` against a fresh initialized in-memory Geschichte repository."
  ([f] (with-repo {:backend :memory :id (random-uuid)} f))
  ([store f]
   (let [cfg {:store store
              :schema-flexibility :write
              :keep-history? true
              :commit-graph? true}]
     (d/create-database cfg)
     (let [conn (d/connect cfg)]
       (try
         (repo/init! conn)
         (f conn)
         (finally
           (d/release conn)
           (d/delete-database cfg)))))))

(defn- commit-file!
  "Write, stage and commit one path. Returns the commit id."
  [conn path content message]
  (repo/write! conn path (->bytes content))
  (repo/stage-all! conn)
  (:geschichte.commit/id (repo/commit! conn {:message message :author "test"})))

(defn- delete-file! [conn path message]
  (repo/remove! conn path)
  (repo/stage-all! conn)
  (:geschichte.commit/id (repo/commit! conn {:message message :author "test"})))

(defn- physical-view [conn]
  (let [db @conn]
    {:config (:config db) :head (p/snapshot-id (gy/create conn))
     :refs (query/refs db) :work (query/worktree db) :index (query/stage db)}))

(deftest historical-overlay-owns-one-private-workspace-and-leaves-parent-unchanged
  (with-repo
    (fn [conn]
      (let [a (commit-file! conn "state.txt" "A" "A")
            system (gy/create conn {:system-name "history"})]
        (repo/write! conn "state.txt" (->bytes "B"))
        (repo/stage! conn ["state.txt"])
        (repo/write! conn "state.txt" (->bytes "B2") {:mode 33261})
        (repo/write! conn "untracked.txt" (->bytes "untracked"))
        (let [ordinary (p/overlay system {})]
          (try
            (let [dirty (physical-view (gy/connection ordinary))]
              (is (= "B2" (text (repo/read (gy/connection ordinary) "state.txt"))))
              (is (= 33261 (get-in dirty [:work "state.txt" :mode])))
              (is (= 33188 (get-in dirty [:index "state.txt" :mode])))
              (is (contains? (:work dirty) "untracked.txt")))
            (commit-file! conn "state.txt" "C" "C")
            (repo/write! conn "staged-extra.txt" (->bytes "extra"))
            (repo/stage! conn ["staged-extra.txt"])
            (repo/write! conn "later-untracked.txt" (->bytes "later"))
            (let [before (physical-view conn)
                  branches (workspace/list conn)
                  historical (p/overlay system {:snapshot (str a) :mode :frozen})]
              (try
                (let [child (gy/connection historical)]
                  (is (true? (:snapshot-overlay (p/capabilities system))))
                  (is (not (identical? conn child)))
                  (is (= (inc (count branches)) (count (workspace/list conn))))
                  (is (= (str a) (p/base-ref historical)))
                  (is (= :frozen (:mode historical)))
                  (is (= #{"state.txt"} (set (keys (query/worktree @child)))))
                  (is (= #{"state.txt"} (set (keys (query/stage @child)))))
                  (is (= "A" (text (repo/read child "state.txt"))))
                  (is (= before (physical-view conn)))
                  (is (= "B2" (text (repo/read (gy/connection ordinary) "state.txt")))))
                (finally (p/discard! historical {})))
              (is (= branches (workspace/list conn)))
              (is (= before (physical-view conn))))
            (finally (p/discard! ordinary {}))))))))

(deftest historical-base-and-nested-overlays-retain-the-canonical-commit
  (with-repo
    (fn [conn]
      (let [a (commit-file! conn "base.txt" "A" "A")
            system (gy/create conn)]
        (commit-file! conn "parent.txt" "C" "parent C")
        (let [historical (p/overlay system {:snapshot a})
              child-system (overlay/overlay-system historical)
              child (gy/connection historical)]
          (try
            (commit-file! conn "later.txt" "D" "parent D")
            (is (= (str a) (p/base-ref historical)))
            (commit-file! child "child.txt" "child" "child work")
            (let [diff (p/diff child-system a (parse-uuid (p/snapshot-id child-system)))
                  nested (p/overlay child-system {:snapshot a :mode :frozen})]
              (try
                (is (= [{:status :added :path "child.txt"}] (:files diff)))
                (is (= (str a) (p/base-ref nested)))
                (is (= "A" (text (repo/read (gy/connection nested) "base.txt"))))
                (is (nil? (repo/read (gy/connection nested) "child.txt")))
                (is (= "child" (text (repo/read child "child.txt"))))
                (finally (p/discard! nested {}))))
            (p/merge-down! historical {})
            (is (= "child" (text (repo/read conn "child.txt"))))
            (is (= "C" (text (repo/read conn "parent.txt"))))
            (is (= "D" (text (repo/read conn "later.txt"))))
            (is (= (str a) (p/base-ref historical)))
            (finally (p/discard! historical {}))))))))

(deftest invalid-historical-request-refuses-before-physical-allocation
  (with-repo
    (fn [conn]
      (commit-file! conn "base.txt" "A" "A")
      (let [system (gy/create conn) calls (atom 0) original workspace/fork!]
        (with-redefs [workspace/fork! (fn [& args] (swap! calls inc) (apply original args))]
          (doseq [opts [{:snapshot nil} {:snapshot "unknown"}
                        {:snapshot (p/snapshot-id system) :mode :following}
                        {:snapshot (p/snapshot-id system) :mode nil}]]
            (is (thrown? clojure.lang.ExceptionInfo (p/overlay system opts))))
          (is (zero? @calls)))
        (is (empty? (workspace/list conn)))))))

(deftest historical-materialization-failure-cleans-acknowledged-owned-resources
  (with-repo
    (fn [conn]
      (let [a (commit-file! conn "state.txt" "A" "A")
            system (gy/create conn)
            before (physical-view conn)
            failure (ex-info "materialization failed" {})
            seen (atom nil)]
        (with-redefs [repo/reset! (fn [child commit opts]
                                    (reset! seen child)
                                    (is (not (identical? child conn)))
                                    (throw failure))]
          (let [result (try (p/overlay system {:snapshot a})
                            (catch Throwable error error))]
            (is (identical? failure result))))
        (is (some? @seen))
        (is (empty? (workspace/list conn)))
        (is (= before (physical-view conn)))))))

(deftest historical-failed-cleanup-retains-the-primary-error-and-exact-owner
  (with-repo
    (fn [conn]
      (let [a (commit-file! conn "state.txt" "A" "A")
            primary (ex-info "materialization refused" {})
            cleanup (ex-info "branch removal refused" {})
            system (gy/create conn)
            result (with-redefs [repo/reset! (fn [& _] (throw primary))
                                 versioning/delete-branch! (fn [& _] (throw cleanup))]
                     (try (p/overlay system {:snapshot a})
                          (catch Throwable error error)))
            owner (cleanup-test/owner-of result)
            resource (cleanup-test/state-of owner)]
        (is (identical? primary (ex-cause result)))
        (is (identical? cleanup (ex-cause (first (:cleanup-errors (ex-data result))))))
        (is (= :child-open (:stage resource)))
        (is (identical? conn (:cleanup-conn resource)))
        (is (some? (:child-conn resource)))
        (is (= #{(:branch resource)} (workspace/list conn)))
        ;; Explicit recovery of the exact retained resource after recording
        ;; uncertainty, not a fabricated successful constructor result.
        (cleanup-test/dispose-known! owner)
        (is (empty? (workspace/list conn)))))))

(deftest overlay-disposal-releases-creation-owned-connection-not-a-reseated-parent
  (with-repo
    (fn [conn]
      (commit-file! conn "state.txt" "A" "A")
      (let [system (gy/create conn)
            overlay (p/overlay system {})
            owned (gy/connection overlay)
            released (atom [])
            release d/release]
        (reset! (:local-writes overlay) system)
        (with-redefs [d/release (fn [c] (swap! released conj c) (release c))]
          (p/discard! overlay {}))
        (is (= 2 (count @released)))
        (is (identical? owned (first @released)))
        (is (identical? conn (second @released)) "Release only our canonical lease, not the caller's")
        (is (not (identical? conn (first @released))))
        (is (empty? (workspace/list conn)))
        (is (= "A" (text (repo/read conn "state.txt"))))))))

(deftest historical-lost-connect-ack-retains-branch-with-live-native-connection
  (with-repo
    (fn [conn]
      (let [a (commit-file! conn "state.txt" "A" "A")
            before (physical-view conn)
            connected (atom nil)
            connect d/connect
            failure (ex-info "connect reply lost" {})
            error (with-redefs [d/connect (fn [cfg & args]
                                            (let [child (apply connect cfg args)]
                                              (if (= :db (:branch cfg)) child
                                                  (do (reset! connected child) (throw failure)))))]
                    (try (p/overlay (gy/create conn) {:snapshot a})
                         (catch Throwable error error)))
            owner (cleanup-test/owner-of error)
            resource (cleanup-test/state-of owner)]
        (try
          (is (identical? failure (ex-cause error)))
          (is (= :branch-open (:stage resource)))
          (is (= :child-connect (:uncertain resource)))
          (is (nil? (:child-conn resource)) "Missing ACK is not proof native connect never ran")
          (is (= #{(:branch resource)} (workspace/list conn)))
          (is (= "A" (text (repo/read @connected "state.txt"))))
          (is (= before (physical-view conn)))
          (finally
            ;; Only this diagnostic observer has the exact lost-ACK connection.
            ;; Recover it explicitly after restoring the native functions.
            (cleanup-test/dispose-known! owner @connected)))))))

(deftest historical-constructor-release-failure-keeps-owned-branch-and-registry
  (with-repo
    (fn [conn]
      (let [a (commit-file! conn "state.txt" "A" "A")
            before (physical-view conn)
            connected (atom nil)
            release d/release
            primary (ex-info "reset failed" {})
            cleanup (ex-info "release failed" {})
            error (with-redefs [repo/reset! (fn [child & _]
                                              (reset! connected child)
                                              (throw primary))
                                d/release (fn [c]
                                            (if (identical? c @connected)
                                              (throw cleanup)
                                              (release c)))]
                    (try (p/overlay (gy/create conn) {:snapshot a})
                         (catch Throwable error error)))
            owner (cleanup-test/owner-of error)
            resource (cleanup-test/state-of owner)]
        (try
          (is (identical? primary (ex-cause error)))
          (is (= :child-open (:stage resource)))
          (is (identical? cleanup (ex-cause (first (:cleanup-errors (ex-data error))))))
          (is (identical? @connected (:child-conn resource)))
          (is (query/initialized? @@connected) "The refused release still has a live native DB")
          (is (= #{(:branch resource)} (workspace/list conn)))
          (is (= before (physical-view conn)))
          (finally
            (cleanup-test/dispose-known! owner)))))))

;; ============================================================
;; Branches and overlays (moved from geschichte.yggdrasil-test)
;; ============================================================

(deftest logical-branches-and-physical-overlays-remain-distinct
  (with-repo
    (fn [conn]
      (commit-file! conn "base.txt" "base\n" "base")
      (let [system (gy/create conn {:system-name "demo"})]
        (is (identical? conn (gy/connection system)))
        (is (= [(get-in @conn [:config :store :id]) :db]
               (gy/workspace-id system)))
        (is (= #{:main} (p/branches system)))
        (is (= :main (p/current-branch system)))
        (p/branch! system :feature)
        (is (= #{:main :feature} (p/branches system)))

        (let [observer (p/overlay system {})
              producer (p/overlay system {})
              local (overlay/overlay-system producer)]
          (is (identical? (:conn local) (gy/connection producer)))
          (is (not= (gy/workspace-id system)
                    (gy/workspace-id producer)))
          (is (= :frozen (:mode producer)))
          (is (= :main (p/current-branch local)))
          (repo/write! (:conn local) "published.txt" (->bytes "published\n"))
          (repo/stage-all! (:conn local))
          (p/commit! local "published")
          (let [published-id (p/snapshot-id local)
                parent (p/merge-down! producer {})]
            (p/discard! producer {})
            (is (= published-id (p/snapshot-id parent)))
            (is (= "published\n"
                   (text (repo/read-at conn
                                       (parse-uuid published-id)
                                       "published.txt"))))
            (p/advance! observer {})
            (is (= published-id
                   (p/snapshot-id (overlay/overlay-system observer))))
            (is (= "published\n"
                   (text (repo/read (:conn (overlay/overlay-system observer))
                                    "published.txt"))))
            (is (identical? parent (p/discard! observer {})))))))))

;; A parent that advanced after its overlay forked cannot take a fast-forward
;; publication. merge-down! used to fail there ("Publication is not a
;; fast-forward"), so a workspace fork was unmergeable once its parent moved.
(deftest merge-down-of-a-diverged-overlay-merges-three-way
  (with-repo
    (fn [conn]
      (commit-file! conn "base.txt" "base\n" "base")
      (let [system (gy/create conn {:system-name "demo"})
            producer (p/overlay system {})
            local (overlay/overlay-system producer)]
        (commit-file! conn "parent.txt" "parent\n" "parent advances")
        (repo/write! (:conn local) "fork.txt" (->bytes "fork\n"))
        (repo/stage-all! (:conn local))
        (p/commit! local "fork work")
        (let [fork-tip (parse-uuid (p/snapshot-id local))
              parent-tip (:geschichte.commit/id (repo/head-commit conn))
              parent (p/merge-down! producer {})
              head (repo/commit-by-id conn (parse-uuid (p/snapshot-id parent)))]
          (p/discard! producer {})
          (is (= #{fork-tip parent-tip}
                 (set (map :geschichte.commit/id (:geschichte.commit/parents head))))
              "a merge commit of both sides")
          (is (= "fork\n" (text (repo/read conn "fork.txt"))))
          (is (= "parent\n" (text (repo/read conn "parent.txt"))))
          (is (not-any? #(str/includes? % "ygg-merge-down") (keys (repo/refs conn)))
              "the temporary ref is gone"))))))

(deftest merge-down-of-a-conflicting-diverged-overlay-is-refused
  (with-repo
    (fn [conn]
      (commit-file! conn "shared.txt" "base\n" "base")
      (let [system (gy/create conn {:system-name "demo"})
            producer (p/overlay system {})
            local (overlay/overlay-system producer)]
        (commit-file! conn "shared.txt" "parent\n" "parent edits")
        (repo/write! (:conn local) "shared.txt" (->bytes "fork\n"))
        (repo/stage-all! (:conn local))
        (p/commit! local "fork edits")
        (let [before (:geschichte.commit/id (repo/head-commit conn))]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unresolved conflicts"
                                (p/merge-down! producer {})))
          (is (= before (:geschichte.commit/id (repo/head-commit conn))) "the parent is unchanged")
          (is (= "parent\n" (text (repo/read conn "shared.txt"))))
          (is (not-any? #(str/includes? % "ygg-merge-down") (keys (repo/refs conn)))))
        (p/discard! producer {})))))

;; ============================================================
;; Snapshotable / Graphable
;; ============================================================

(deftest parent-ids-reports-the-tip-s-actual-parents
  ;; `repo/head-commit` pulls the REF TARGET, whose pull pattern carries only
  ;; :db/id, :geschichte.commit/id and :geschichte.commit/snapshot — no
  ;; :geschichte.commit/parents. Reading parents straight off it silently
  ;; answers #{} for every repository, so the tip must be re-resolved.
  (with-repo
    (fn [conn]
      (let [first-id (commit-file! conn "a.txt" "one\n" "one")
            _ (commit-file! conn "a.txt" "two\n" "two")
            system (gy/create conn {})]
        (testing "the trap itself: the ref-target pull carries no parents"
          (let [tip (repo/head-commit conn)]
            (is (empty? (:geschichte.commit/parents tip)))
            (is (seq (:geschichte.commit/parents
                      (repo/commit-by-id conn (:geschichte.commit/id tip))))
                "…while the same commit, re-resolved, does")))
        (is (= #{(str first-id)} (p/parent-ids system)))))))

(deftest common-ancestor-finds-the-merge-base
  (with-repo
    (fn [conn]
      (let [base (commit-file! conn "base.txt" "base\n" "base")
            system (gy/create conn {})]
        (p/branch! system :feature)
        (commit-file! conn "main.txt" "main\n" "main work")
        (p/checkout system :feature)
        (commit-file! conn "feature.txt" "feature\n" "feature work")
        (is (= (str base) (p/common-ancestor system :main :feature)))))))

;; ============================================================
;; Mergeable / diff
;; ============================================================

(defn- diverged
  "base.txt on both sides; main adds main.txt, feature edits base.txt, adds
   feature.txt and deletes gone.txt. Returns the system on branch :main."
  [conn]
  (commit-file! conn "base.txt" "base\n" "base")
  (commit-file! conn "gone.txt" "gone\n" "gone")
  (let [system (gy/create conn {:system-name "demo"})]
    (p/branch! system :feature)
    (commit-file! conn "main.txt" "main\n" "main work")
    (p/checkout system :feature)
    (commit-file! conn "base.txt" "base\nextra\n" "feature edits base")
    (commit-file! conn "feature.txt" "feature\n" "feature work")
    (delete-file! conn "gone.txt" "feature deletes gone")
    (p/checkout system :main)
    system))

(deftest diff-reports-a-file-change-set-and-a-patch
  (with-repo
    (fn [conn]
      (let [system (diverged conn)
            result (p/diff system :main :feature)]
        (testing "it is a GeschichteDiff, not a datom diff of the internal schema"
          (is (instance? yggdrasil.types.GeschichteDiff result)))

        (testing "files carries GitDiff's shape, so consumers need no special case"
          (is (= #{{:status :modified :path "base.txt"}
                   {:status :added :path "feature.txt"}
                   {:status :deleted :path "gone.txt"}
                   {:status :deleted :path "main.txt"}}
                 (set (:files result)))))

        (testing "the patch is a real unified diff of file CONTENT"
          (is (str/includes? (:patch result) "+++ b/base.txt"))
          (is (str/includes? (:patch result) "+extra"))
          (is (str/includes? (:patch result) "+feature")))

        (testing "stat and summary agree with the change set"
          (is (str/includes? (:stat result) "4 files changed"))
          (is (= 4 (:files-changed (:summary result))))
          (is (pos? (:insertions (:summary result))))
          (is (pos? (:deletions (:summary result)))))

        (testing "snapshots are the resolved commit ids, not the branch keywords"
          (is (= (p/snapshot-id system) (:snapshot-a result)))
          (is (some? (parse-uuid (:snapshot-b result)))))))))

(deftest diff-can-skip-patch-rendering
  ;; The change set is a pure comparison of two path maps; only the patch reads
  ;; blobs. A caller listing changes should not pay for content.
  (with-repo
    (fn [conn]
      (let [system (diverged conn)
            result (p/diff system :main :feature {:patch? false})]
        (is (= 4 (count (:files result))))
        (is (= "" (:patch result)))
        (is (= "" (:stat result)))))))

(deftest diff-summarizes-binary-content-instead-of-rendering-it
  (with-repo
    (fn [conn]
      (commit-file! conn "keep.txt" "keep\n" "base")
      (let [system (gy/create conn {})]
        (p/branch! system :feature)
        (p/checkout system :feature)
        (repo/write! conn "blob.bin" (byte-array [0 1 2 0 3]))
        (repo/stage-all! conn)
        (repo/commit! conn {:message "binary" :author "test"})
        (p/checkout system :main)
        (let [result (p/diff system :main :feature)]
          (is (= [{:status :added :path "blob.bin"}] (:files result)))
          (is (str/includes? (:patch result) "Binary files differ")))))))

(deftest diff-of-an-unknown-snapshot-is-a-diff-error
  (with-repo
    (fn [conn]
      (commit-file! conn "a.txt" "a\n" "a")
      (let [system (gy/create conn {})
            result (p/diff system :main :nope)]
        (is (instance? yggdrasil.types.DiffError result))
        (is (str/includes? (:error result) "Unknown"))))))

;; ============================================================
;; Mergeable / conflicts + merge!
;; ============================================================

(deftest conflicts-are-empty-for-disjoint-changes
  (with-repo
    (fn [conn]
      (let [system (diverged conn)]
        (is (= [] (p/conflicts system :main :feature)))))))

(deftest conflicts-report-the-path-both-sides-changed-differently
  (with-repo
    (fn [conn]
      (commit-file! conn "shared.txt" "base\n" "base")
      (let [system (gy/create conn {})]
        (p/branch! system :feature)
        (commit-file! conn "shared.txt" "main version\n" "main edit")
        (p/checkout system :feature)
        (commit-file! conn "shared.txt" "feature version\n" "feature edit")
        (p/checkout system :main)
        (let [conflicts (p/conflicts system :main :feature)]
          (is (= 1 (count conflicts)))
          (is (instance? yggdrasil.types.Conflict (first conflicts)))
          (is (= "shared.txt" (:path (first conflicts))))
          (is (not= (:ours (first conflicts)) (:theirs (first conflicts)))))))))

(deftest merge-brings-the-source-branch-s-files-onto-the-target
  (with-repo
    (fn [conn]
      (let [system (diverged conn)
            before (p/snapshot-id system)]
        (p/merge! system :feature)
        (is (not= before (p/snapshot-id system)) "a merge commit was made")
        (let [tree (repo/tree conn :head)]
          (testing "additions and edits from both sides are present"
            (is (contains? tree "main.txt"))
            (is (contains? tree "feature.txt"))
            (is (= "base\nextra\n" (text (repo/read conn "base.txt")))))
          (testing "and the source branch's deletion propagated"
            (is (not (contains? tree "gone.txt")))))
        (testing "the merge commit records both parents"
          (is (= 2 (count (p/parent-ids system)))))))))

(deftest merge-refuses-a-conflicting-source
  (with-repo
    (fn [conn]
      (commit-file! conn "shared.txt" "base\n" "base")
      (let [system (gy/create conn {})]
        (p/branch! system :feature)
        (commit-file! conn "shared.txt" "main version\n" "main edit")
        (p/checkout system :feature)
        (commit-file! conn "shared.txt" "feature version\n" "feature edit")
        (p/checkout system :main)
        (let [before (p/snapshot-id system)
              thrown (try (p/merge! system :feature)
                          nil
                          (catch clojure.lang.ExceptionInfo e e))]
          (is (some? thrown) "a conflicting merge must not silently no-op")
          (is (= :geschichte/merge-conflict (:type (ex-data thrown))))
          (is (seq (:conflicts (ex-data thrown))))
          (testing "and the repository is untouched by the refusal"
            (is (= before (p/snapshot-id system)))
            (is (= "main version\n" (text (repo/read conn "shared.txt"))))))))))

(deftest merging-an-already-merged-branch-is-a-no-op
  (with-repo
    (fn [conn]
      (commit-file! conn "base.txt" "base\n" "base")
      (let [system (gy/create conn {})]
        (p/branch! system :feature)
        ;; :feature is an ancestor of :main — geschichte's own planner would
        ;; still build an empty merge commit here.
        (commit-file! conn "main.txt" "main\n" "main work")
        (let [before (p/snapshot-id system)]
          (is (identical? system (p/merge! system :feature)))
          (is (= before (p/snapshot-id system))))))))

;; ============================================================
;; Capabilities
;; ============================================================

(deftest capabilities-advertise-mergeability
  (with-repo
    (fn [conn]
      (commit-file! conn "a.txt" "a\n" "a")
      (let [caps (p/capabilities (gy/create conn {}))]
        (is (instance? yggdrasil.types.Capabilities caps))
        (is (:mergeable caps)
            "the adapter now implements Mergeable — consumers gate on this flag")
        (is (satisfies? p/Mergeable (gy/create conn {})))))))

(deftest disjoint-line-edits-merge-through-the-adapter
  ;; geschichte resolves a path from its tree ENTRIES, so two branches that both
  ;; touched one file used to conflict whether or not the changes overlapped.
  ;; With the content merge wired into `merge-plan`, only real overlaps reach
  ;; `conflicts` — which is what a consumer builds a review surface on.
  (with-repo
    (fn [conn]
      (repo/write! conn "f.txt" (gbytes/utf8 "one\ntwo\nthree\nfour\n"))
      (repo/stage-all! conn)
      (repo/commit! conn {:message "base" :author "t"})
      (let [sys (gy/create conn {:system-name "adapter-merge"})]
        (p/branch! sys :side :main)
        ;; main edits the first line
        (repo/write! conn "f.txt" (gbytes/utf8 "ONE\ntwo\nthree\nfour\n"))
        (repo/stage-all! conn)
        (repo/commit! conn {:message "main" :author "t"})
        ;; side edits the last
        (p/checkout sys :side)
        (repo/write! conn "f.txt" (gbytes/utf8 "one\ntwo\nthree\nFOUR\n"))
        (repo/stage-all! conn)
        (repo/commit! conn {:message "side" :author "t"})
        (p/checkout sys :main)

        (testing "no conflict is reported for edits that do not overlap"
          (is (empty? (p/conflicts sys :main :side))))

        (testing "and the merge lands BOTH edits"
          (p/merge! sys :side)
          (is (= "ONE\ntwo\nthree\nFOUR\n"
                 (String. (repo/read conn "f.txt")))))))))

(deftest competing-edits-to-one-line-still-conflict-through-the-adapter
  (with-repo
    (fn [conn]
      (repo/write! conn "g.txt" (gbytes/utf8 "a\nold\nc\n"))
      (repo/stage-all! conn)
      (repo/commit! conn {:message "base" :author "t"})
      (let [sys (gy/create conn {:system-name "adapter-conflict"})]
        (p/branch! sys :side :main)
        (repo/write! conn "g.txt" (gbytes/utf8 "a\nMAIN\nc\n"))
        (repo/stage-all! conn)
        (repo/commit! conn {:message "main" :author "t"})
        (p/checkout sys :side)
        (repo/write! conn "g.txt" (gbytes/utf8 "a\nSIDE\nc\n"))
        (repo/stage-all! conn)
        (repo/commit! conn {:message "side" :author "t"})
        (p/checkout sys :main)

        (is (seq (p/conflicts sys :main :side))
            "a real overlap must still reach a person")
        (is (thrown? clojure.lang.ExceptionInfo (p/merge! sys :side)))))))

(defn- physical-checkpoint [db]
  {:version 1 :kind :datahike :commit (d/commit-id db)
   :source {:store (get-in db [:config :store :id])
            :repository (:geschichte.repo/id (query/repository db))}})

(deftest exact-checkpoint-retains-the-entire-repository-state-without-resetting-parent
  (with-repo
    (fn [conn]
      (let [logical (commit-file! conn "state.txt" "C1" "C1")]
        (d/transact conn [{:db/ident :fixture/value :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
        (let [eid (:db/id (query/repository @conn))]
          (d/transact conn [{:db/id eid :fixture/value "C1"}])
          (repo/write! conn "staged.txt" (->bytes "C1 staged"))
          (repo/stage-all! conn)
          (repo/write! conn "untracked.txt" (->bytes "C1 untracked"))
          (let [c1 @conn origin (physical-checkpoint c1)
                later (commit-file! conn "state.txt" "C2" "C2")]
            (repo/create-ref! conn "refs/heads/future")
            (repo/write! conn "future.txt" (->bytes "Only C2"))
            (d/transact conn [{:db/id eid :fixture/value "C2"}])
            (let [c2 @conn branches (d/branches conn)
                  child (p/overlay-at-checkpoint (gy/create conn) origin)
                  selected (gy/connection child)]
              (try
                (is (= (vec (d/datoms c1 :eavt)) (vec (d/datoms @selected :eavt))))
                (is (= (query/stage c1) (query/stage @selected)))
                (is (= (query/refs c1) (query/refs @selected)))
                (is (= (:schema c1) (:schema @selected)))
                (is (= (:secondary-index-keys c1) (:secondary-index-keys @selected)))
                (is (= (str logical) (p/base-ref child)))
                (is (= origin (:checkpoint child)))
                (is (= "C1" (text (repo/read selected "state.txt"))))
                (is (= "C1 untracked" (text (repo/read selected "untracked.txt"))))
                (is (nil? (repo/read selected "future.txt")))
                (commit-file! selected "own.txt" "Child only" "Own work")
                (is (nil? (repo/read conn "own.txt")))
                (is (thrown? clojure.lang.ExceptionInfo (p/merge-down! child)) "Dirty parent is still refused")
                (is (= (vec (d/datoms c2 :eavt)) (vec (d/datoms @conn :eavt))))
                (finally (p/discard! child)))
              (is (= branches (d/branches conn))))))))))

(deftest checkpoint-child-merge-keeps-logical-ancestry-and-newer-parent-work
  (with-repo
    (fn [conn]
      (commit-file! conn "left.txt" "C1 left" "C1 left")
      (commit-file! conn "right.txt" "C1 right" "C1 right")
      (let [origin (physical-checkpoint @conn)]
        (commit-file! conn "right.txt" "Parent C2" "Parent work")
        (let [child (p/overlay-at-checkpoint (gy/create conn) origin)]
          (try
            (commit-file! (gy/connection child) "left.txt" "Child work" "Child work")
            (p/merge-down! child)
            (is (= "Child work" (text (repo/read conn "left.txt"))))
            (is (= "Parent C2" (text (repo/read conn "right.txt"))))
            (finally (p/discard! child))))))))

(deftest invalid-checkpoints-refuse-before-owning-or-allocating-native-resources
  (with-repo
    (fn [conn]
      (commit-file! conn "state.txt" "C1" "C1")
      (let [origin (physical-checkpoint @conn) system (gy/create conn)]
        (with-redefs [cleanup-owner/acquire! (fn [& _] (throw (AssertionError. "No native allocation for invalid origin")))]
          (doseq [opts [{:checkpoint (assoc-in origin [:source :store] (random-uuid))}
                        {:checkpoint (assoc-in origin [:source :repository] (random-uuid))}
                        {:checkpoint (assoc origin :commit (random-uuid))}
                        {:checkpoint origin :mode :following}
                        {:checkpoint origin :snapshot (p/snapshot-id system)}
                        {:checkpoint (assoc origin :native :handle)}]]
            (is (thrown? clojure.lang.ExceptionInfo (p/overlay system opts))))
          (let [db (assoc-in @conn [:schema :fixture/building] {:db.secondary/status :building})]
            (with-redefs [versioning/commit-as-db (fn [& _] db)]
              (is (= :yggdrasil.adapters.geschichte/checkpoint-building-index
                     (try (p/overlay-at-checkpoint system origin) nil
                          (catch clojure.lang.ExceptionInfo error (:type (ex-data error)))))))))))))

(deftest checkpoint-lost-allocation-ack-retains-its-exact-cleanup-owner
  (with-repo
    (fn [conn]
      (commit-file! conn "state.txt" "C1" "C1")
      (let [origin (physical-checkpoint @conn) before (physical-view conn)
            allocate versioning/branch! failure (ex-info "allocation reply lost" {})
            error (with-redefs [versioning/branch! (fn [& args] (apply allocate args) (throw failure))]
                    (try (p/overlay-at-checkpoint (gy/create conn) origin)
                         (catch Throwable error error)))
            owner (cleanup-test/owner-of error) resource (cleanup-test/state-of owner)]
        (try
          (is (identical? failure (ex-cause error)))
          (is (= :canonical-open (:stage resource)))
          (is (= :branch-allocation (:uncertain resource)))
          (is (= #{(:branch resource)} (workspace/list conn)))
          (is (= before (physical-view conn)))
          (finally (cleanup-test/dispose-known! owner)))))))

(deftest checkpoint-lost-connect-ack-keeps-the-native-branch-and-connection
  (with-repo
    (fn [conn]
      (commit-file! conn "state.txt" "C1" "C1")
      (let [origin (physical-checkpoint @conn) before (physical-view conn)
            connected (atom nil) connect d/connect failure (ex-info "connect reply lost" {})
            error (with-redefs [d/connect (fn [cfg & args]
                                            (let [child (apply connect cfg args)]
                                              (if (= :db (:branch cfg)) child
                                                  (do (reset! connected child) (throw failure)))))]
                    (try (p/overlay-at-checkpoint (gy/create conn) origin)
                         (catch Throwable error error)))
            owner (cleanup-test/owner-of error) resource (cleanup-test/state-of owner)]
        (try
          (is (identical? failure (ex-cause error)))
          (is (= :branch-open (:stage resource)))
          (is (= :child-connect (:uncertain resource)))
          (is (nil? (:child-conn resource)))
          (is (= "C1" (text (repo/read @connected "state.txt"))))
          (is (= before (physical-view conn)))
          (finally (cleanup-test/dispose-known! owner @connected)))))))

(deftest checkpoint-post-connect-verification-failure-has-owned-cleanup
  (doseq [cleanup-fails? [false true]]
    (with-repo
      (fn [conn]
        (commit-file! conn "state.txt" "C1" "C1")
        (let [origin (physical-checkpoint @conn) before (physical-view conn)
              connect cleanup-owner/connect-child! remove-branch versioning/delete-branch!
              cleanup-failure (ex-info "removal refused" {})
              error (with-redefs [cleanup-owner/connect-child!
                                  (fn [& args]
                                    (let [child (apply connect args)]
                                      (repo/write! child "unexpected.txt" (->bytes "Changed before return"))
                                      child))
                                  versioning/delete-branch!
                                  (fn [& args] (if cleanup-fails? (throw cleanup-failure) (apply remove-branch args)))]
                      (try (p/overlay-at-checkpoint (gy/create conn) origin)
                           (catch Throwable error error)))]
          (if cleanup-fails?
            (let [owner (cleanup-test/owner-of error) resource (cleanup-test/state-of owner)]
              (try
                (is (= :yggdrasil.adapters.geschichte/checkpoint-child-changed (:type (ex-data (ex-cause error)))))
                (is (seq (:cleanup-errors (ex-data error))))
                (is (= #{(:branch resource)} (workspace/list conn)))
                (finally (cleanup-test/dispose-known! owner))))
            (do
              (is (= :yggdrasil.adapters.geschichte/checkpoint-child-changed (:type (ex-data error))))
              (is (empty? (workspace/list conn)))))
          (is (= before (physical-view conn))))))))

(deftest checkpoint-destination-collision-never-deletes-or-overwrites-the-existing-branch
  (with-repo
    (fn [conn]
      (commit-file! conn "state.txt" "C1" "C1")
      (let [origin (physical-checkpoint @conn) collision :geschichte.workspace/existing-checkpoint-test]
        (workspace/fork! conn collision)
        (let [before (versioning/branch-as-db conn collision)
              error (with-redefs [workspace/branch-key (constantly collision)]
                      (try (p/overlay-at-checkpoint (gy/create conn) origin)
                           (catch Throwable error error)))
              owner (cleanup-test/owner-of error) resource (cleanup-test/state-of owner)]
          (try
            (is (= :branch-already-exists (:type (ex-data (ex-cause error)))))
            (is (= :branch-allocation (:uncertain resource)))
            (is (= #{collision} (workspace/list conn)))
            (is (= (vec (d/datoms before :eavt))
                   (vec (d/datoms (versioning/branch-as-db conn collision) :eavt))))
            (finally
              ;; The injected collision tells this test the pre-existing branch
              ;; was not allocated by the failed constructor. Release its extra
              ;; canonical lease only; production must retain uncertainty.
              (d/release (:cleanup-conn resource))
              (d/delete-branch! conn collision))))))))

(deftest checkpoint-verification-rejects-different-primary-state-with-the-same-commit-metadata
  (with-repo
    (fn [conn]
      (commit-file! conn "state.txt" "C1" "C1")
      (let [origin (physical-checkpoint @conn) before (physical-view conn)
            connect cleanup-owner/connect-child!
            error (with-redefs [cleanup-owner/connect-child!
                                (fn [& args]
                                  (let [native (apply connect args)
                                        db @native
                                        changed (:db-after (d/with db [[:db/add (:db/id (query/repository db))
                                                                        :geschichte.repo/head "refs/heads/corrupt"]]))
                                        malformed (assoc db :eavt (:eavt changed))]
                                    (is (= (d/commit-id db) (d/commit-id malformed)))
                                    (is (= (:hash db) (:hash malformed)))
                                    ;; The owner still retains the exact real
                                    ;; connection for cleanup; this bad returned
                                    ;; view must never escape the constructor.
                                    (reify clojure.lang.IDeref (deref [_] malformed))))]
                    (try (p/overlay-at-checkpoint (gy/create conn) origin)
                         (catch Throwable error error)))]
        (is (= :yggdrasil.adapters.geschichte/checkpoint-child-changed (:type (ex-data error))))
        (is (empty? (workspace/list conn)))
        (is (= before (physical-view conn)))))))

(deftest incompatible-checkpoint-execution-refuses-before-source-io
  (with-repo
    (fn [conn]
      (commit-file! conn "state.txt" "C1" "C1")
      (let [system (gy/create conn) origin (physical-checkpoint @conn)]
        (is (p/supports-checkpoint-overlay? system {:sync? true}))
        (is (not (p/supports-checkpoint-overlay? system {:sync? false})))
        (with-redefs [versioning/commit-as-db (fn [& _] (throw (AssertionError. "No source IO for incompatible execution")))]
          (doseq [sync? [false nil :yes]]
            (is (thrown? clojure.lang.ExceptionInfo (p/overlay-at-checkpoint system origin {:sync? sync?})))
            (is (thrown? clojure.lang.ExceptionInfo (p/overlay system {:checkpoint origin :sync? sync?})))))))))

(deftest checkpoint-uses-persisted-primary-root-addresses-on-a-file-store
  (let [directory (.toFile (java.nio.file.Files/createTempDirectory
                            "ygg-checkpoint-" (make-array java.nio.file.attribute.FileAttribute 0)))]
    (try
      (with-repo {:backend :file :id (random-uuid) :path (str (.getPath directory) "/store")}
        (fn [conn]
          (commit-file! conn "state.txt" "C1" "C1")
          (let [c1 @conn origin (physical-checkpoint c1)
                keys [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet]
                roots (mapv #(audit/-merkle-root (get c1 %)) keys)]
            (is (every? some? roots))
            (commit-file! conn "state.txt" "C2" "C2")
            (let [child (p/overlay-at-checkpoint (gy/create conn) origin)]
              (try
                (is (= roots (mapv #(audit/-merkle-root (get @(gy/connection child) %)) keys)))
                (is (= "C1" (text (repo/read (gy/connection child) "state.txt"))))
                (is (= "C2" (text (repo/read conn "state.txt"))))
                (finally (p/discard! child)))))))
      (finally
        (when (.exists directory)
          (doseq [file (reverse (file-seq directory))] (io/delete-file file true)))))))

(deftest checkpoint-validation-is-read-only-and-returns-only-the-original-descriptor
  (with-repo
    (fn [conn]
      (commit-file! conn "state.txt" "C1" "C1")
      (let [origin (physical-checkpoint @conn) system (gy/create conn)
            before (physical-view conn) branches (d/branches conn)
            refuse (fn [& _] (throw (AssertionError. "Read-only validation acquired native resources")))]
        (with-redefs [cleanup-owner/owner refuse cleanup-owner/acquire! refuse
                      versioning/branch! refuse workspace/fork! refuse
                      d/connect refuse d/transact refuse d/transact! refuse]
          (is (p/supports-checkpoint-validation? system))
          (is (identical? origin (p/validate-checkpoint-origin! system origin)))
          (doseq [bad [(assoc-in origin [:source :store] (random-uuid))
                       (assoc-in origin [:source :repository] (random-uuid))
                       (assoc origin :commit (random-uuid))
                       (assoc origin :native :handle)]]
            (is (thrown? clojure.lang.ExceptionInfo (p/validate-checkpoint-origin! system bad))))
          (doseq [opts [{:sync? false} {:sync? nil} {:mode :following} {:unknown true}]]
            (is (thrown? clojure.lang.ExceptionInfo (p/validate-checkpoint-origin! system origin opts))))
          (let [db (assoc-in @conn [:schema :fixture/building] {:db.secondary/status :building})]
            (with-redefs [versioning/commit-as-db (fn [& _] db)]
              (is (= :yggdrasil.adapters.geschichte/checkpoint-building-index
                     (try (p/validate-checkpoint-origin! system origin) nil
                          (catch clojure.lang.ExceptionInfo error (:type (ex-data error)))))))))
        (is (= before (physical-view conn)))
        (is (= branches (d/branches conn)))))))

(deftest checkpoint-construction-rechecks-a-previously-validated-origin
  (with-repo
    (fn [conn]
      (commit-file! conn "state.txt" "C1" "C1")
      (let [origin (physical-checkpoint @conn) system (gy/create conn)]
        (is (= origin (p/validate-checkpoint-origin! system origin)))
        (with-redefs [versioning/commit-as-db (fn [& _] nil)
                      cleanup-owner/owner (fn [& _] (throw (AssertionError. "Unavailable source must not allocate")))]
          (is (= :yggdrasil.adapters.geschichte/checkpoint-unavailable
                 (try (p/overlay-at-checkpoint system origin) nil
                      (catch clojure.lang.ExceptionInfo error (:type (ex-data error)))))))))))
