(ns yggdrasil.convergent.merging-ormap
  "Merging OR-Map — an OR-Map whose concurrent per-key values are FOLDED into a
   single value by a user-supplied merge function, instead of surfaced as a value-
   set the way the plain `yggdrasil.convergent.ormap` does. Ported from
   replikativ.crdt.merging-ormap.

   value = {:adds {k {uid v}} :removals {k {uid v}}} — same two grow-only maps as
   the OR-Map. The difference is the value side:
   - assoc k v → REUSE the key's existing uid (mint one only for a fresh key) and
                 FOLD v into it via `merge-fn` (so repeated local writes accumulate
                 at one uid), rather than minting a fresh tag per write.
   - get   k   → reduce the live uids' values through `merge-fn` to ONE value.
   - join      → a DEEP merge: `merge-with (merge-with merge-fn)` on :adds (so two
                 replicas' same-uid values fold; different-uid values both survive
                 and fold at read), `merge-with merge` on :removals.

   CONVERGENCE CONTRACT: `merge-fn` MUST be commutative, associative and idempotent
   (a join-semilattice lub — LWW-by-timestamp, numeric max, set-union, deep entity-
   merge). Then the fold is order-independent and the map is a CRDT. It is wrapped
   to absorb nils (`(merge-fn nil v) ⇒ v`) so it seeds the read-time reduce.

   Unlike replikativ's global merge-code REGISTRY, the (wrapped) `merge-fn` rides in
   the system's `:config` and travels with the value (survives `-join`) — so reads
   and joins both have it. For cross-peer sync, construct every replica with the
   SAME merge-fn (the analogue of replikativ's shared merge-code)."
  (:refer-clojure :exclude [assoc dissoc get keys])
  (:require [clojure.set :as set]
            [yggdrasil.convergent.system :as sys]))

(def ^:private bottom {:adds {} :removals {}})

(defn- wrap
  "Absorb nils so `merge-fn` seeds the read-time reduce and the deep join."
  [merge-fn]
  (fn [a b] (cond (nil? a) b (nil? b) a :else (merge-fn a b))))

(defn- merging-join
  "Least-upper-bound of two Merging-OR-Map states under wrapped `wmfn`."
  [wmfn]
  (fn [a b]
    {:adds     (merge-with (partial merge-with wmfn) (:adds a) (:adds b))
     :removals (merge-with merge (:removals a) (:removals b))}))

(defn merging-ormap
  "A Merging-OR-Map conflict-free system folding concurrent per-key values with
   `merge-fn` (commutative/associative/idempotent — the convergence contract)."
  [id merge-fn & {:keys [branch] :or {branch :main}}]
  (let [wmfn (wrap merge-fn)]
    (sys/conflict-free-system id :merging-ormap :branch branch
                              :vjoin (merging-join wmfn) :bottom bottom
                              :config {:merge-fn wmfn})))

(defn- wmfn-of [m] (:merge-fn (:config m)))

(defn- live-uids [val k]
  (set/difference (set (clojure.core/keys (clojure.core/get-in val [:adds k])))
                  (set (clojure.core/keys (clojure.core/get-in val [:removals k])))))

(defn assoc
  "Assoc `v` under `k`, FOLDING it into the key's value via `merge-fn` (local op)."
  [m k v]
  (let [wmfn   (wmfn-of m)
        val    (sys/cur m)
        adds-k (clojure.core/get-in val [:adds k])
        uid    (or (ffirst adds-k) (random-uuid))
        merged (wmfn (clojure.core/get adds-k uid) v)]
    (sys/record-delta (sys/upd! m clojure.core/assoc-in [:adds k uid] merged)
                      {:adds {k {uid merged}} :removals {}})))

(defn dissoc
  "Remove `k` — tombstone its currently-live tags (local op)."
  [m k]
  (let [val   (sys/cur m)
        uids  (live-uids val k)
        rem'  (reduce (fn [r uid] (clojure.core/assoc r uid nil))
                      (clojure.core/get-in val [:removals k]) uids)]
    (sys/record-delta
     (sys/upd! m clojure.core/assoc-in [:removals k] rem')
     {:adds {} :removals {k rem'}})))

(defn get
  "The single merged value for `k` (live uids folded through `merge-fn`), or nil."
  [m k]
  (let [wmfn (wmfn-of m)
        val  (sys/cur m)
        cut  (live-uids val k)]
    (when (seq cut)
      ;; sort for a deterministic fold order (belt-and-braces; merge-fn is
      ;; commutative, so order shouldn't matter, but uuids sort cheaply).
      (reduce wmfn nil (map (clojure.core/get-in val [:adds k]) (sort cut))))))

(defn keys
  "The set of keys with at least one live value."
  [m]
  (let [val (sys/cur m)]
    (into #{} (filter #(seq (live-uids val %))) (clojure.core/keys (:adds val)))))
