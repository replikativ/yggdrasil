(ns yggdrasil.convergent.commit-dag-test
  "The flat-CRDT commit-DAG: commit builder determinism (Step 0), then graph persistence +
   merge-base + convergent-join as the later steps land."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.test-async :refer [deftest-async <? sync? file-cfg]]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.dag :as dag]
            [yggdrasil.convergent.gset :as g]
            [yggdrasil.convergent.orset :as orset]
            [yggdrasil.convergent :as c]
            [yggdrasil.protocols :as p]
            [hasch.core :as hasch]
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [is.simm.partial-cps.async :refer [async]])))

(deftest flat-commit-builder-is-canonical
  (testing "make-commit is deterministic + parent-order-independent (convergent id)"
    (let [addr  (str (hasch/uuid {:some "root"}))
          addr2 (str (hasch/uuid {:other "root"}))
          c1    (d/make-commit addr #{"p1" "p2"})]
      (is (= (:id c1) (:id (d/make-commit addr #{"p2" "p1"})))
          "parent SET order does not change the id")
      (is (= (:id c1) (:id (d/make-commit addr ["p2" "p1"])))
          "parent coll type / order irrelevant (sorted canonically)")
      (is (not= (:id c1) (:id (d/make-commit addr #{"p1"})))
          "different parents ⇒ different id")
      (is (not= (:id c1) (:id (d/make-commit addr2 #{"p1" "p2"})))
          "different root ⇒ different id")
      (is (= {:root addr :parents ["p1" "p2"]} (:value c1))
          "value is the anonymous, canonically-sorted {:root :parents}")))
  (testing "base-commit is a fixed shared sentinel (a guaranteed common ancestor)"
    (is (= (:id (d/base-commit)) (:id (d/base-commit))) "base id is stable across calls")
    (is (= {:root nil :parents []} (:value (d/base-commit))))))

(deftest-async commit-persist-and-walk
  (testing "store commits as STANDALONE content-addressed keys (O(1), no PSS grow-set); COLD-read
            each commit's parents — the walkable lineage merge-base rides on."
    (let [{:keys [kv-store]} (<? (d/open (file-cfg) {} {:sync? sync?}))
          base (d/base-commit)
          c1   (d/make-commit "root1" #{(:id base)})
          c2   (d/make-commit "root2" #{(:id c1)})
          _    (<? (d/append-commit! kv-store base {:sync? sync?}))
          _    (<? (d/append-commit! kv-store c1 {:sync? sync?}))
          _    (<? (d/append-commit! kv-store c2 {:sync? sync?}))
          pof  (d/commit-parents-of kv-store {:sync? sync?})]
      (is (= #{(:id c1)}   (set (<? (pof (:id c2))))) "c2's parents = {c1}")
      (is (= #{(:id base)} (set (<? (pof (:id c1))))) "c1's parents = {base}")
      (is (empty? (<? (pof (:id base)))) "base has no parents")
      (is (nil? (<? (pof "nonexistent"))) "an absent commit reads nil")
      ;; the dag algebra linearizes the lineage via parents-of
      (is (= #{(:id base) (:id c1) (:id c2)}
             (set (<? (dag/commit-history pof (:id c2) {:sync? sync?}))))
          "commit-history walks the full ancestry of the tip"))))

(deftest-async gset-commit-builds-walkable-history
  (testing "conj + commit! twice builds base→c1→c2; history + ancestor? + merge-base walk it"
    (let [g0   (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          base (:commit g0)
          g1c  (<? (p/commit! (<? (g/conj g0 :a {:sync? sync?})) nil {:sync? sync?}))
          g2c  (<? (p/commit! (<? (g/conj g1c :b {:sync? sync?})) nil {:sync? sync?}))
          hist (set (<? (p/history g2c {:sync? sync?})))]
      (is (some? base) "factory seeded the base commit tip")
      (is (not= base (:commit g1c)) "commit! advanced the tip off base")
      (is (not= (:commit g1c) (:commit g2c)) "second commit advanced again")
      (is (= 3 (count hist)) "history = base + 2 commits")
      (is (contains? hist base) "base is in the tip's history")
      (is (contains? hist (:commit g2c)) "tip is in its own history")
      (is (<? (p/ancestor? g2c base (:commit g2c) {:sync? sync?})) "base is an ancestor of the tip")
      (is (= (:commit g2c) (<? (g/merge-base g2c :main :main {:sync? sync?})))
          "merge-base of a branch with itself = its tip"))))

(deftest-async gset-branch-diverge-merge-base
  (testing "fork + diverge → merge-base = the fork-point commit; merge! authors a merge commit
            whose parents are both branch tips and whose value is the union."
    (let [g0      (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          main1   (<? (p/commit! (<? (g/conj g0 :m1 {:sync? sync?})) nil {:sync? sync?}))
          fork-pt (:commit main1)
          _       (<? (p/branch! main1 :fork :main {:sync? sync?}))
          fork1   (<? (p/commit! (<? (g/conj (<? (p/checkout main1 :fork {:sync? sync?})) :f1 {:sync? sync?}))
                                 nil {:sync? sync?}))
          main2   (<? (p/commit! (<? (g/conj (<? (p/checkout fork1 :main {:sync? sync?})) :m2 {:sync? sync?}))
                                 nil {:sync? sync?}))]
      (is (= fork-pt (<? (g/merge-base main2 :main :fork {:sync? sync?})))
          "merge-base(main, fork) = the fork-point commit")
      (let [merged (<? (p/merge! main2 :fork {:sync? sync? :flush? true}))
            info   (<? (p/commit-info merged (:commit merged) {:sync? sync?}))]
        (is (= #{(:commit main2) (:commit fork1)} (:parent-ids info))
            "merge commit parents = {main tip, fork tip}")
        (is (= #{:m1 :f1 :m2} (<? (g/elements merged {:sync? sync?})))
            "merged value = union of both branches")))))

(deftest-async gset-join-union-only-is-p2p-safe
  (testing "-join merges VALUES only — idempotent, authors NO commit — so automatic p2p sync
            converges to a FIXED POINT with no runaway lineage growth; explicit commits are the
            only lineage-writers, and the commit-graph still syncs cross-store via merge-peer!'s
            (idempotent grow-set) union."
    (let [a    (<? (p/commit! (<? (g/conj (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?})) :a1 {:sync? sync?})) nil {:sync? sync?}))
          b    (<? (p/commit! (<? (g/conj (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?})) :b1 {:sync? sync?})) nil {:sync? sync?}))
          tipA (:commit a) tipB (:commit b)
          j1   (<? (c/-join a b {:sync? sync? :flush? false}))
          j2   (<? (c/-join j1 b {:sync? sync? :flush? false}))
          j3   (<? (c/-join j2 a {:sync? sync? :flush? false}))]
      (is (= #{:a1 :b1} (<? (g/elements j1 {:sync? sync?}))) "-join value = union of the peers")
      (is (= tipA (:commit j1)) "-join authored NO commit (tip stays the local commit)")
      ;; NO RUNAWAY: repeated joins after convergence add nothing — same value, same tip (fixed point)
      (is (= tipA (:commit j2) (:commit j3)) "further joins author no commits — a fixed point")
      (is (= #{:a1 :b1} (<? (g/elements j3 {:sync? sync?}))) "value stable across repeated joins")
      ;; lineage still propagates cross-store via merge-peer! — WITHOUT any -join authoring
      (let [a2 (<? (g/merge-peer! a b {:sync? sync?}))]
        (is (some? (<? (p/commit-info a2 tipB {:sync? sync?})))
            "merge-peer! carried b's commit into a's graph (grow-set union, idempotent)")))))

(deftest-async orset-two-half-commit-dag
  (testing "two-half (OR-Set) commit-DAG via the shared helpers: commit! builds history,
            branch/diverge → merge-base = fork point, merge! authors a two-parent merge commit."
    (let [o0      (<? (orset/orset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          base    (:commit o0)
          main1   (<? (p/commit! (<? (orset/conj o0 :m1 {:sync? sync?})) nil {:sync? sync?}))
          fork-pt (:commit main1)
          _       (<? (p/branch! main1 :fork :main {:sync? sync?}))
          fork1   (<? (p/commit! (<? (orset/conj (<? (p/checkout main1 :fork {:sync? sync?})) :f1 {:sync? sync?}))
                                 nil {:sync? sync?}))
          main2   (<? (p/commit! (<? (orset/conj (<? (p/checkout fork1 :main {:sync? sync?})) :m2 {:sync? sync?}))
                                 nil {:sync? sync?}))]
      (is (some? base) "factory seeded the base commit")
      (is (not= base fork-pt) "commit! advanced the tip off base")
      (is (= fork-pt (<? (orset/merge-base main2 :main :fork {:sync? sync?})))
          "merge-base(main, fork) = the fork-point commit")
      (let [merged (<? (p/merge! main2 :fork {:sync? sync? :flush? true}))
            info   (<? (p/commit-info merged (:commit merged) {:sync? sync?}))]
        (is (= #{(:commit main2) (:commit fork1)} (:parent-ids info)) "merge parents = both tips")
        (is (= #{:m1 :f1 :m2} (<? (orset/elements merged {:sync? sync?}))) "merged value = union")))))

(def ^:private epoch-date  #?(:clj (java.util.Date. 0)             :cljs (js/Date. 0)))
(def ^:private future-date #?(:clj (java.util.Date. 9999999999999) :cljs (js/Date. 9999999999999)))

(deftest-async gset-gc-retention-datahike-style
  (testing "commit-reachable-roots retains ALL historical commit roots at the epoch cutoff
            (safe default — reclaims nothing), and TRUNCATES to just the tip's root past a
            future cutoff (the datahike window: recent commit roots survive, old ones swept)."
    (let [g1  (<? (p/commit! (<? (g/conj (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?})) :a {:sync? sync?})) nil {:sync? sync?}))
          g2  (<? (p/commit! (<? (g/conj g1 :b {:sync? sync?})) nil {:sync? sync?}))
          g3  (<? (p/commit! (<? (g/conj g2 :c {:sync? sync?})) nil {:sync? sync?}))
          r1  (some-> (:root (<? (p/commit-info g3 (:commit g1) {:sync? sync?}))) str parse-uuid)
          r3  (some-> (:root (<? (p/commit-info g3 (:commit g3) {:sync? sync?}))) str parse-uuid)
          tips [(:commit g3)]
          all-roots    (<? (d/commit-reachable-roots (:kv-store g3) (:storage g3) tips epoch-date (:config g3) {:sync? sync?}))
          future-roots (<? (d/commit-reachable-roots (:kv-store g3) (:storage g3) tips future-date (:config g3) {:sync? sync?}))]
      (is (contains? all-roots r1) "epoch cutoff retains an OLD commit's root (nothing reclaimed)")
      (is (contains? all-roots r3) "epoch cutoff retains the tip's root")
      (is (contains? future-roots r3) "future cutoff still retains the tip's root")
      (is (not (contains? future-roots r1)) "future cutoff TRUNCATES the old commit's root (sweepable)"))))

(deftest-async gset-cold-peer-lineage-syncs
  (testing "the commit-DAG ships to a COLD peer via merge-peer! (grow-set union of the reserved
            :commit-graph branch), so peer B reads A's full lineage from its OWN store —
            commit-info, ancestry, and common-ancestor of A's commit-ids all resolve on B."
    (let [a1       (<? (p/commit! (<? (g/conj (<? (g/gset "A" {:store-config (file-cfg)} {:sync? sync?})) :m1 {:sync? sync?})) nil {:sync? sync?}))
          fork-pt  (:commit a1)
          _        (<? (p/branch! a1 :fork :main {:sync? sync?}))
          af       (<? (p/commit! (<? (g/conj (<? (p/checkout a1 :fork {:sync? sync?})) :f1 {:sync? sync?})) nil {:sync? sync?}))
          am       (<? (p/commit! (<? (g/conj (<? (p/checkout af :main {:sync? sync?})) :m2 {:sync? sync?})) nil {:sync? sync?}))
          a-main   (:commit am) a-fork (:commit af)
          b        (<? (g/merge-peer! (<? (g/gset "B" {:store-config (file-cfg)} {:sync? sync?})) am {:sync? sync?}))]
      (is (some? (<? (p/commit-info b a-main {:sync? sync?}))) "B received A's main-tip commit")
      (is (some? (<? (p/commit-info b a-fork {:sync? sync?}))) "B received A's fork-tip commit")
      (is (<? (p/ancestor? b fork-pt a-main {:sync? sync?})) "B walks A's ancestry (fork-pt → main tip)")
      (is (= fork-pt (<? (p/common-ancestor b a-main a-fork {:sync? sync?})))
          "B computes the fork-point as the common ancestor of A's two tips — from its own synced graph"))))

(deftest-async orset-cold-peer-lineage-syncs
  (testing "two-half (OR-Set) cold-peer lineage sync — exercises the C1 fix (two-half-merge-peer!
            shipping the single-root :commit-graph branch)."
    (let [a1       (<? (p/commit! (<? (orset/conj (<? (orset/orset "A" {:store-config (file-cfg)} {:sync? sync?})) :m1 {:sync? sync?})) nil {:sync? sync?}))
          fork-pt  (:commit a1)
          _        (<? (p/branch! a1 :fork :main {:sync? sync?}))
          af       (<? (p/commit! (<? (orset/conj (<? (p/checkout a1 :fork {:sync? sync?})) :f1 {:sync? sync?})) nil {:sync? sync?}))
          am       (<? (p/commit! (<? (orset/conj (<? (p/checkout af :main {:sync? sync?})) :m2 {:sync? sync?})) nil {:sync? sync?}))
          a-main   (:commit am) a-fork (:commit af)
          b        (<? (orset/merge-peer! (<? (orset/orset "B" {:store-config (file-cfg)} {:sync? sync?})) am {:sync? sync?}))]
      (is (some? (<? (p/commit-info b a-main {:sync? sync?}))) "B received A's main-tip commit (via C1 fix)")
      (is (some? (<? (p/commit-info b a-fork {:sync? sync?}))) "B received A's fork-tip commit")
      (is (= fork-pt (<? (p/common-ancestor b a-main a-fork {:sync? sync?})))
          "B computes the fork-point from its own synced two-half commit-graph"))))
