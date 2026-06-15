(ns yggdrasil.convergent.system
  "Generic CONFLICT-FREE yggdrasil system, parameterized by a value-level join.

   Every CRDT in the catalog (G-Set, LWW-Register, OR-Map, …) is the same system
   machinery — SystemIdentity / Snapshotable / Branchable / Mergeable (conflicts
   ⇒ []) / PConvergent (`-join`) over a `{branch -> value}` map — and differs ONLY
   in `vjoin` (the value-level least-upper-bound) and `bottom` (the empty value).
   So a new CRDT is a few lines: a constructor + value read/write helpers; the
   convergence + branch + fork semantics come for free.

   `vjoin` MUST be commutative, associative, idempotent (that's what makes the
   system conflict-free).

   VALUE SEMANTICS: the `store` is a PLAIN immutable map; every mutator returns a
   NEW system value rather than modifying in place (the `yggdrasil.protocols`
   contract). The ONLY mutable cell in the CRDT model is the signal-atom that
   HOLDS a system value (the FRP write boundary) — never the system itself, so a
   deref is a real snapshot and a mutation never entangles another handle."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]))

(declare ->ConflictFreeSystem apply-delta)

(defrecord ConflictFreeSystem [id stype store current vjoin bottom]
  ;; store : a plain {branch -> value} map, rebuilt copy-on-write by each mutator
  ;; (never mutated in place).
  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] stype)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :graphable false :overlayable false})

  p/Snapshotable
  (snapshot-id [_] (str (hash [current (get store current)])))
  (parent-ids [_] #{})
  (as-of [_ _] (get store current)) (as-of [_ _ _] (get store current))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  p/Branchable
  ;; VALUE-SEMANTIC: every structural op returns a NEW system over a new store map.
  (branches [_] (set (keys store)))
  (branches [_ _] (set (keys store)))
  (current-branch [_] current)
  (branch! [this name]        (assoc this :store (assoc store name (get store current bottom))))
  (branch! [this name from]   (assoc this :store (assoc store name (get store from bottom))))
  (branch! [this name from _] (assoc this :store (assoc store name (get store from bottom))))
  (delete-branch! [this name]   (assoc this :store (dissoc store name)))
  (delete-branch! [this name _] (assoc this :store (dissoc store name)))
  (checkout [this name] (assoc this :current name))
  (checkout [this name _] (assoc this :current name))

  p/Mergeable
  ;; branch-merge IS the value join (conflict-free) — same join the symmetric
  ;; -join uses, just source = another branch. Returns a NEW system.
  (merge! [this source] (p/merge! this source nil))
  (merge! [this source _]
    (assoc this :store (update store current (fn [v] (vjoin (or v bottom) (get store source bottom))))))
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {})

  c/PConvergent
  ;; symmetric peer join: per-branch value-join of two replicas' stores (pure).
  ;; IDEMPOTENCE: a join that changes nothing returns the receiver IDENTICAL (so a
  ;; signal holding this CRDT doesn't re-fire on a no-op). On change, a FRESH
  ;; system carrying NO δ (remote-integrated content never re-propagates).
  (-join [this other]
    (let [joined (merge-with vjoin store (:store other))]
      (if (= joined store)
        this
        (->ConflictFreeSystem id stype joined current vjoin bottom))))
  (-conflict-free? [_] true)

  c/PDeltaApply
  (-apply-delta [this delta] (apply-delta this delta)))

(defn conflict-free-system
  "Construct a conflict-free system. `vjoin`/`bottom` define the CRDT."
  [id stype & {:keys [branch init vjoin bottom] :or {branch :main}}]
  (->ConflictFreeSystem id stype {branch (if (nil? init) bottom init)}
                        branch vjoin bottom))

;; value-level accessors (operate on the current branch's value)
(defn cur "Current branch's value." [s] (get (:store s) (:current s) (:bottom s)))
(defn put! "Set the current branch's value. Returns a NEW system (value-semantic)." [s v]
  (assoc s :store (assoc (:store s) (:current s) v)))
(defn upd! "Update the current branch's value with f. Returns a NEW system (value-semantic)." [s f & args]
  (assoc s :store (apply update (:store s) (:current s) (fnil f (:bottom s)) args)))

;; δ-state (OP perspective) — shared by every in-mem CRDT (G-Set/LWWR/OR-Map).
(defn record-delta
  "Record that a local op contributed δ-value `d` — accrued into the system's
   local δ via its own `vjoin` (the δ is a value of the same shape `vjoin`
   consumes). Mutators call this after their `put!`/`upd!`. Carries δ in metadata
   (preserved across the value-semantic `assoc`)."
  [s d]
  (c/with-delta s (:vjoin s) d))

(defn apply-delta
  "Consume a peer's δ — `vjoin` it into the current value (the OP-path; cf. -join
   the STATE-path). Returns a NEW system."
  [s d]
  (upd! s (:vjoin s) d))
