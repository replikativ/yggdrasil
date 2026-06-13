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
   replay context the tag source must be injected."
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
  (set/difference (set (keys (get-in val [:adds k])))
                  (set (keys (get-in val [:removals k])))))

(defn assoc-key
  "Assoc `v` under `k` with a fresh observed tag (local op)."
  [m k v]
  (sys/upd! m (fn [val] (assoc-in val [:adds k (random-uuid)] v))))

(defn dissoc-key
  "Remove `k` — tombstone its currently-live tags (local op)."
  [m k]
  (sys/upd! m (fn [val]
                (reduce (fn [val uid]
                          (assoc-in val [:removals k uid] (get-in val [:adds k uid])))
                        val
                        (live-uids val k)))))

(defn lookup
  "The live value-set for `k` (add-wins), or nil if absent."
  [m k]
  (let [val (sys/cur m)
        cut (live-uids val k)]
    (when (seq cut)
      (set (map (get-in val [:adds k]) cut)))))

(defn ormap-keys
  "The set of keys with at least one live value."
  [m]
  (let [val (sys/cur m)]
    (into #{} (filter #(seq (live-uids val %))) (keys (:adds val)))))
