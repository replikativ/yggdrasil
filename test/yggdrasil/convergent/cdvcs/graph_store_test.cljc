(ns yggdrasil.convergent.cdvcs.graph-store-test
  "ORACLE property test: the store-backed (`async+sync`) commit-graph algebra
   (`graph-store`) must agree with the pure in-memory algebra (`graph`) on random
   DAGs — for `lowest-common-ancestors`, `remove-ancestors` (the downstream head
   recompute) and `commit-history`. The `parents-of` accessor is an in-mem map
   wrapped in `async+sync` (a value under `:sync? true`, a continuation under
   `:sync? false`) — exactly the contract the real PSS-slice accessor satisfies.
   Portable: JVM-sync + cljs-async."
  (:require [clojure.test :refer [is testing]]
            [yggdrasil.test-async :refer [deftest-async <? sync?]]
            [yggdrasil.convergent.cdvcs.graph :as graph]
            [yggdrasil.convergent.cdvcs.graph-store :as gs]
            #?(:clj  [is.simm.partial-cps.async :refer [async]])
            #?(:clj  [yggdrasil.macros :refer [async+sync]])
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(defn- mk
  "An in-mem `parents-of` accessor over graph map `g`, honouring `opts` `:sync?` —
   the same value/continuation contract the real PSS-slice accessor has."
  [g opts]
  (fn [id] (async+sync (:sync? opts) (async (get g id)))))

;; ---- random connected DAGs (parents are always EARLIER ids ⇒ acyclic; commit 0
;;      is the sole root, every later commit has ≥1 parent ⇒ connected) ----------
(defn- rand-dag [n]
  (loop [i 1 g {0 []}]
    (if (>= i n)
      g
      (let [k       (inc (rand-int 2))                ; 1 or 2 parents
            parents (vec (distinct (repeatedly k #(rand-int i))))]
        (recur (inc i) (assoc g i parents))))))

(defn- heads-of [g]
  (let [referenced (into #{} (mapcat val) g)]
    (into #{} (remove referenced) (keys g))))

(deftest-async lca-agrees-with-oracle
  (testing "store LCA :lcas == pure LCA :lcas over random DAGs (single merged graph)"
    (dotimes [_ 25]
      (let [g    (rand-dag (+ 6 (rand-int 18)))
            ids  (vec (keys g))
            a    (nth ids (rand-int (count ids)))
            b    (nth ids (rand-int (count ids)))
            pure (:lcas (graph/lowest-common-ancestors g #{a} g #{b}))
            got  (:lcas (<? (gs/lowest-common-ancestors (mk g {:sync? sync?}) #{a}
                                                        (mk g {:sync? sync?}) #{b}
                                                        {:sync? sync?})))]
        (is (= pure got) (str "LCA mismatch g=" g " a=" a " b=" b))))))

(deftest-async remove-ancestors-agrees-with-oracle
  (testing "store remove-ancestors == pure (the downstream head recompute)"
    (dotimes [_ 25]
      (let [g    (rand-dag (+ 6 (rand-int 18)))
            ids  (vec (keys g))
            ha   (set (take (inc (rand-int 2)) (shuffle ids)))
            hb   (set (take (inc (rand-int 2)) (shuffle ids)))
            pure (graph/remove-ancestors g g ha hb)
            got  (<? (gs/remove-ancestors (mk g {:sync? sync?}) (mk g {:sync? sync?})
                                          ha hb {:sync? sync?}))]
        (is (= pure got) (str "remove-ancestors mismatch g=" g " ha=" ha " hb=" hb))))))

(deftest-async commit-history-agrees-with-oracle
  (testing "store commit-history == pure DFS linearisation"
    (dotimes [_ 25]
      (let [g    (rand-dag (+ 6 (rand-int 18)))
            head (rand-int (count g))
            pure (graph/commit-history g head)
            got  (<? (gs/commit-history (mk g {:sync? sync?}) head {:sync? sync?}))]
        (is (= pure got) (str "commit-history mismatch g=" g " head=" head))))))

(deftest-async downstream-end-to-end-agrees
  (testing "store union+remove-ancestors == pure downstream over two divergent views"
    (dotimes [_ 25]
      (let [base   (rand-dag (+ 4 (rand-int 8)))
            ;; two divergent extensions sharing `base` — DISJOINT id spaces (100+/200+)
            ;; so the two peers never collide on an id (real ids are unique hashes).
            ext    (fn [offset]
                     (loop [g base i offset added 0 n (inc (rand-int 3))]
                       (if (>= added n)
                         g
                         (recur (assoc g i [(rand-nth (vec (keys g)))])
                                (inc i) (inc added) n))))
            ga     (ext 100)
            gb     (ext 200)
            sa     {:commit-graph ga :heads (heads-of ga) :version 1}
            sb     {:commit-graph gb :heads (heads-of gb) :version 1}
            pure   (graph/downstream sa sb)
            merged (merge ga gb)
            got    (<? (gs/remove-ancestors (mk merged {:sync? sync?}) (mk ga {:sync? sync?})
                                            (:heads sa) (:heads sb) {:sync? sync?}))]
        (is (= (:heads pure) got)
            (str "downstream heads mismatch ga=" ga " gb=" gb))))))
