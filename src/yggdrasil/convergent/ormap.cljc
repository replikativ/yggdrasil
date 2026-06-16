(ns yggdrasil.convergent.ormap
  "Observed-Remove Map (OR-Map) — an add-wins keyed CRDT, as a conflict-free
   yggdrasil system. Ported from replikativ.crdt.ormap.

   value = {:adds {k {uid v}} :removals {k {uid v}}}.
   - assoc k v   → a FRESH tag `uid` under `[:adds k]` (so a re-add after an
                   observed remove survives — add-wins).
   - dissoc k    → tombstone the currently-live uids of k (copy into :removals).
   - lookup k    → the live value-SET: `(set/difference (keys adds[k]) (keys removals[k]))`
                   (multi-value if concurrent writers; one value in the common case).
   - join (vjoin)→ `merge-with merge` on :adds and :removals (commutative,
                   associative, idempotent — globally-unique uids make it a CRDT).

   NB (O5): `uid` uses random-uuid for global uniqueness; in a deterministic
   replay context the tag source must be injected — and MUST be value-inclusive
   (distinct values ⇒ distinct uids). The inner `merge` is right-biased on a uid
   collision, so two replicas assoc'ing the SAME k with the SAME uid but DIFFERENT
   v would make the join order-dependent (non-commutative). random-uuid and a
   content-hash-of-[k v] tag both satisfy this; a content-hash-of-k-only does not.

   NB: callers alias this ns (`[… :as om]`) and call `om/assoc`/`om/get`/`om/dissoc`/
   `om/keys`, so the bare Clojure verbs read like collection ops; only this defining
   ns excludes them from `clojure.core` and qualifies the few internal core uses."
  (:refer-clojure :exclude [assoc dissoc get keys])
  (:require [clojure.set :as set]
            [yggdrasil.convergent.system :as sys]))

(def ^:private bottom {:adds {} :removals {}})

(defn ormap-join
  "Least-upper-bound of two OR-Map states (replikativ `downstream`)."
  [a b]
  {:adds (merge-with merge (:adds a) (:adds b))
   :removals (merge-with merge (:removals a) (:removals b))})

(defn ormap
  "An OR-Map conflict-free system."
  [id & {:keys [branch] :or {branch :main}}]
  (sys/conflict-free-system id :ormap :branch branch :vjoin ormap-join :bottom bottom))

(defn- live-uids [val k]
  (set/difference (set (clojure.core/keys (get-in val [:adds k])))
                  (set (clojure.core/keys (get-in val [:removals k])))))

(defn assoc
  "Assoc `v` under `k` with a fresh observed tag (local op)."
  [m k v]
  (let [uid (random-uuid)]
    (sys/record-delta (sys/upd! m assoc-in [:adds k uid] v)
                      {:adds {k {uid v}}})))

(defn dissoc
  "Remove `k` — tombstone its currently-live tags (local op)."
  [m k]
  (let [val   (sys/cur m)
        uids  (live-uids val k)
        delta {:removals {k (into {} (map (fn [uid] [uid (get-in val [:adds k uid])])) uids)}}]
    (sys/record-delta
     (sys/upd! m (fn [val]
                   (reduce (fn [val uid]
                             (assoc-in val [:removals k uid] (get-in val [:adds k uid])))
                           val uids)))
     delta)))

(defn get
  "The live value-set for `k` (add-wins), or nil if absent."
  [m k]
  (let [val (sys/cur m)
        cut (live-uids val k)]
    (when (seq cut)
      (set (map (get-in val [:adds k]) cut)))))

(defn keys
  "The set of keys with at least one live value."
  [m]
  (let [val (sys/cur m)]
    (into #{} (filter #(seq (live-uids val %))) (clojure.core/keys (:adds val)))))
