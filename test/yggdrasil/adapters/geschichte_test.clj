(ns yggdrasil.adapters.geschichte-test
  "Behaviour of the Geschichte adapter on the JVM.

   `Mergeable` is the reason this adapter moved into yggdrasil: while it lived
   in geschichte it implemented SystemIdentity/Snapshotable/Branchable/Graphable
   but NOT Mergeable, so a Geschichte-backed system had no `diff` at all and
   consumers fell back to a bare datom count of geschichte's INTERNAL
   commit/tree/blob schema."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [geschichte.bytes :as gbytes]
            [geschichte.repo :as repo]
            [yggdrasil.adapters.geschichte :as gy]
            [yggdrasil.convergent.overlay :as overlay]
            [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]))

(defn- ->bytes [s] (gbytes/utf8 s))
(defn- text [bs] (when bs (gbytes/decode-utf8 bs)))

(defn- with-repo
  "Run `f` against a fresh initialized in-memory Geschichte repository."
  [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
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
          (d/delete-database cfg))))))

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
