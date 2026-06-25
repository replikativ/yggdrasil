(ns yggdrasil.convergent.cdvcs.graph-store
  "Store-backed (`async+sync`) commit-graph algebra — the SAME LCA / frontier /
   history algorithms as the pure `cdvcs.graph`, but a commit's parents are read
   through an injected accessor `parents-of : id -> (async parents-or-nil)` instead
   of an in-memory map. This lets the commit-graph live in a PSS (read on the fly,
   bounded resident heap) while the pure `cdvcs.graph` stays as the correctness
   ORACLE (the property test asserts the two agree on random DAGs).

   `parents-of` yields the commit's parent-id vector (possibly `[]` for a base
   commit) when the commit is present, or `nil` when absent. Each public fn mirrors
   its pure counterpart 1:1, with `(mapcat graph heads)` → `parents-set` and
   `(select-keys graph ids)`-counting → `count-present` (both awaited)."
  (:require [clojure.set :as set]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(defn- parents-set
  "The union of the parents of every id in `heads` — `(set (mapcat graph heads))`,
   awaited. (async+sync)"
  [parents-of heads opts]
  (async+sync (:sync? opts)
              (async
               (loop [hs (seq heads) acc #{}]
                 (if hs
                   (recur (next hs) (into acc (or (await (parents-of (first hs))) [])))
                   acc)))))

(defn- count-present
  "How many of `ids` are present in the graph — the awaited analogue of
   `(count (select-keys graph ids))`. (async+sync)"
  [parents-of ids opts]
  (async+sync (:sync? opts)
              (async
               (loop [is (seq ids) n 0]
                 (if is
                   (recur (next is) (if (some? (await (parents-of (first is)))) (inc n) n))
                   n)))))

(defn lowest-common-ancestors
  "Online BFS LCA, awaited — mirrors `graph/lowest-common-ancestors`. The two sides
   read through `parents-of-a` / `parents-of-b` (equal for the single-graph case).
   Returns {:lcas :visited-a :visited-b}. (async+sync)"
  [parents-of-a heads-a parents-of-b heads-b opts]
  (async+sync (:sync? opts)
              (async
               (let [ha (set heads-a) hb (set heads-b)]
                 (if (= ha hb)
                   {:lcas ha :visited-a ha :visited-b hb}
                   (loop [heads-a ha visited-a ha start-a ha
                          heads-b hb visited-b hb start-b hb]
                     (let [new-a (await (parents-set parents-of-a heads-a opts))
                           new-b (await (parents-set parents-of-b heads-b opts))
                           va    (set/union visited-a new-a)
                           vb    (set/union visited-b new-b)]
                       (if (and (not-empty new-b)
                                (empty? (set/intersection va new-b)))
                         (recur new-a va start-a new-b vb start-b)
                         (let [lcas    (set/intersection va vb)
                               present (await (count-present parents-of-a new-b opts))]
                           (if (and (not (empty? lcas))
                                    (= present (count new-b)))
                             {:lcas lcas :visited-a va :visited-b vb}
                             (do
                               (when (and (empty? new-a) (empty? new-b))
                                 (throw (ex-info "Graph is not connected, LCA failed."
                                                 {:dangling-heads-a heads-a
                                                  :dangling-heads-b heads-b
                                                  :start-heads-a start-a
                                                  :start-heads-b start-b
                                                  :visited-a va :visited-b vb})))
                               (recur new-a va start-a new-b vb start-b))))))))))))

(defn- pairwise-lcas
  "Awaited `graph/pairwise-lcas`: membership of `heads-b` checked against
   `parents-of-a` (the receiver's ORIGINAL graph); the per-pair LCAs computed over
   `parents-of-merged` (the unioned graph). (async+sync)"
  [parents-of-merged parents-of-a heads-a heads-b opts]
  (async+sync (:sync? opts)
              (async
               (let [present-b (await (count-present parents-of-a heads-b opts))]
                 (if (= (count heads-b) present-b)
                   (set/difference (set heads-b) (set heads-a))
                   (loop [pairs (for [a heads-a b heads-b :when (not= a b)] [a b])
                          acc   #{}]
                     (if (seq pairs)
                       (let [[a b] (first pairs)
                             {:keys [lcas]} (await (lowest-common-ancestors
                                                    parents-of-merged #{a}
                                                    parents-of-merged #{b} opts))]
                         (recur (next pairs) (set/union acc lcas)))
                       acc)))))))

(defn remove-ancestors
  "Awaited `graph/remove-ancestors`: new heads = (heads-a ∪ heads-b) − pairwise LCAs.
   `parents-of-merged` traverses the unioned graph; `parents-of-a` is the receiver's
   original graph (the fast-forward membership test). (async+sync)"
  [parents-of-merged parents-of-a heads-a heads-b opts]
  (async+sync (:sync? opts)
              (async
               (let [to-remove (await (pairwise-lcas parents-of-merged parents-of-a
                                                     heads-a heads-b opts))]
                 (set/difference (set/union (set heads-a) (set heads-b)) to-remove)))))

(defn commit-history
  "Awaited `graph/commit-history`: DFS linearisation of `head`'s ancestry through
   `parents-of`. (async+sync)"
  [parents-of head opts]
  (async+sync (:sync? opts)
              (async
               (loop [hist [] hist-set #{} stack [head]]
                 (let [[f & r] stack]
                   (if f
                     (let [parents (remove hist-set (or (await (parents-of f)) []))]
                       (if (seq parents)
                         (recur hist hist-set (concat parents stack))
                         (recur (if (hist-set f) hist (conj hist f))
                                (conj hist-set f)
                                r)))
                     hist))))))
