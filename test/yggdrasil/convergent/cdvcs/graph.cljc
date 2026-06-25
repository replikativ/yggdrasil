(ns yggdrasil.convergent.cdvcs.graph
  "The PURE commit-graph algebra of a CDVCS — the convergent heart, ported
   verbatim from replikativ.crdt.cdvcs.meta (+ commit-history from realize).

   A CDVCS value is

     {:commit-graph {commit-id [parent-id ...] ...}   ; a grow-only DAG
      :heads        #{commit-id ...}                   ; frontier − ancestors (DERIVED)
      :version      n}

   `downstream` (the join) merges two commit-graphs and recomputes the heads via
   the lowest-common-ancestor cut. It is commutative, associative and idempotent
   on the grow-only graph — i.e. exactly a join-semilattice least-upper-bound —
   so it is yggdrasil's `-join`. Multiple heads after a join are NOT an error:
   that is the lifted conflict (resolve it with an explicit `merge`).

   No store, no async, no clock, no hash — just set algebra over ids. The durable
   wrapping + value verbs live in `yggdrasil.convergent.cdvcs`."
  (:require [clojure.set :as set]))

;; adapted from clojure.set, but faster when the intersection is small
(defn intersection [s1 s2]
  (if (< (count s2) (count s1))
    (recur s2 s1)
    (reduce (fn [result item]
              (if (contains? s2 item)
                (conj result item)
                result))
            #{} s1)))

(defn consistent-graph?
  "Every referenced parent is itself a commit in the graph."
  [graph]
  (let [parents (->> graph vals (map set) (apply set/union #{}))
        commits (->> graph keys set)]
    (set/superset? commits parents)))

(defn lowest-common-ancestors
  "Online BFS, O(n), assumes no cycles. Returns {:lcas :visited-a :visited-b}."
  ([graph-a heads-a graph-b heads-b]
   (cond
     (= (set heads-a) (set heads-b))
     {:lcas (set heads-a) :visited-a (set heads-a) :visited-b (set heads-b)}

     :else
     (lowest-common-ancestors graph-a heads-a heads-a heads-a
                              graph-b heads-b heads-b heads-b)))
  ([graph-a heads-a visited-a start-heads-a
    graph-b heads-b visited-b start-heads-b]
   (let [new-heads-a (set (mapcat graph-a heads-a))
         new-heads-b (set (mapcat graph-b heads-b))
         visited-a (set/union visited-a new-heads-a)
         visited-b (set/union visited-b new-heads-b)]
     ;; short-circuit: keep walking a as long as no new b-head is in visited-a
     (if (and (not-empty new-heads-b)
              (empty? (intersection visited-a new-heads-b)))
       (recur graph-a new-heads-a visited-a start-heads-a
              graph-b new-heads-b visited-b start-heads-b)
       (let [lcas (intersection visited-a visited-b)]
         (if (and (not (empty? lcas))
                  ;; keep going until all of b's paths are in graph-a
                  (= (count (select-keys graph-a new-heads-b))
                     (count new-heads-b)))
           {:lcas lcas :visited-a visited-a :visited-b visited-b}
           (do
             (when (and (empty? new-heads-a) (empty? new-heads-b))
               (throw (ex-info "Graph is not connected, LCA failed."
                               {:dangling-heads-a heads-a
                                :dangling-heads-b heads-b
                                :start-heads-a start-heads-a
                                :start-heads-b start-heads-b
                                :visited-a visited-a
                                :visited-b visited-b})))
             (recur graph-a new-heads-a visited-a start-heads-a
                    graph-b new-heads-b visited-b start-heads-b))))))))

(defn- pairwise-lcas [new-graph graph-a heads-a heads-b]
  (cond
    ;; if heads-b is already in graph-a, then they are the lcas
    (= (count heads-b) (count (select-keys graph-a (seq heads-b))))
    (set/difference heads-b heads-a)

    :else
    (apply set/union #{}
           (for [a heads-a b heads-b
                 :when (not= a b)]
             (let [{:keys [lcas]} (lowest-common-ancestors new-graph #{a} new-graph #{b})]
               lcas)))))

(defn remove-ancestors
  "The new head set = (heads-a ∪ heads-b) − the pairwise LCAs."
  [new-graph graph-a heads-a heads-b]
  (let [to-remove (pairwise-lcas new-graph graph-a heads-a heads-b)]
    (set/difference (set/union heads-a heads-b) to-remove)))

(defn downstream
  "Apply downstream updates from `op` to `cdvcs`. Idempotent and commutative —
   this IS the convergent join on the commit-graph value."
  [{bs :heads cg :commit-graph :as cdvcs}
   {obs :heads ocg :commit-graph}]
  (let [new-graph (if (> (count cg) (count ocg))
                    (merge cg ocg)
                    (merge ocg cg))
        new-heads (remove-ancestors new-graph cg bs obs)]
    (assoc cdvcs
           :heads new-heads
           :commit-graph new-graph)))

(defn commit-history
  "The linear commit history through depth-first linearisation — each commit
   once, the first time it is found. Pure; used to realize a head's value."
  ([commit-graph commit]
   (commit-history commit-graph [] #{} [commit]))
  ([commit-graph hist hist-set stack]
   (let [[f & r] stack
         parents (filter #(not (hist-set %)) (commit-graph f))]
     (if f
       (if-not (empty? parents)
         (recur commit-graph hist hist-set (concat parents stack))
         (recur commit-graph
                (if-not (hist-set f) (conj hist f) hist)
                (conj hist-set f)
                r))
       hist))))
