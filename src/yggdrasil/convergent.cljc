(ns yggdrasil.convergent
  "The conflict-free convergent merge law — the CRDT regime of yggdrasil's
   system algebra.

   `-join` is a SYMMETRIC, 2-way, no-ancestor merge of two **peer** replicas:

     (-join a b) ≡ (-join b a)          ; commutative
     (-join a (-join b c)) ≡ (-join (-join a b) c)  ; associative
     (-join a a) ≡ a                    ; idempotent

   This is deliberately NOT scoped to a parent. A peer is not a child: replicas
   merge symmetrically, any-to-any, with no privileged ancestor. That is the
   whole point of a CRDT and the reason it needs neither a common ancestor nor
   conflict resolution.

   Contrast `yggdrasil.protocols/Mergeable` (`merge!`/`conflicts`/`diff`): that
   is the **hierarchical, branch-oriented, conflict-surfacing** merge of the
   versioned tier (datahike/git) — 3-way, ancestor-based, and historically
   expressed as `merge-to-parent!`. A conflict-free system MAY also satisfy
   Mergeable (its branch-merge is a join, `conflicts` is always empty) so it can
   still ride the tree machinery — but `-join` is its native, symmetric form,
   and the general distributed model merges peers with `-join`, not to a parent."
  (:refer-clojure :exclude []))

(defprotocol PConvergent
  "A system whose value forms a join-semilattice — a CRDT."
  (-join [this other]
    "Least-upper-bound (merge) of this replica with peer `other`. Commutative,
     associative, idempotent. No ancestor, no conflicts, no parent/child.")
  (-conflict-free? [this]
    "True — marks this system as auto-converging (capability dispatch: the
     merge path skips ancestor lookup and never surfaces conflicts)."))

(defn join
  "Fold `-join` over peer replicas → the converged value. Order-independent."
  [replica & more]
  (reduce -join replica more))

(defn convergent?
  "Does `x` participate in conflict-free convergent merge?"
  [x]
  (satisfies? PConvergent x))

;; ============================================================
;; δ-state — the OP perspective (replikativ's op-based CRDT)
;; ============================================================
;; A δ (delta) is a SMALL value of the same shape the CRDT's join consumes — the
;; *change a local mutation made*, captured AT THE WRITE (no diffing). `state ⊔ δ
;; = new-state`, and δs themselves join, so a peer consumes ops by JOINING the δ
;; (convergent under loss/reorder — no causal metadata needed).
;;
;; The δ rides in record METADATA, so it never affects value equality (two
;; replicas with equal content but different in-flight δ are `=`), needs no
;; defrecord field, and is uniform across the whole catalog. Mutators ACCRUE
;; their op (`with-delta`); the consumer (signal layer) CLEARS it at the
;; propagation boundary (`clear-delta`); `-join` returns fresh records that carry
;; no δ, so remote-integrated changes never re-propagate.

(def ^:const delta-key
  "Metadata key under which a replica carries its local, not-yet-propagated δ."
  ::delta)

(defn delta-of
  "The local δ-state (ops applied since the last `clear-delta`) carried by replica
   `x`, or nil. A small value joinable into a peer's state — for a G-Set a set of
   added elements, for a 2P/OR-Set `{:adds.. :removals..}`, for an OR-Map
   `{:adds {k {tag v}}}`, etc."
  [x]
  (when x (get (meta x) delta-key)))

(defn with-delta
  "Return `x` recording that a local op contributed δ `d` — accrued onto any
   existing local δ via `accrue` (the δ's own join, e.g. `set/union` for a G-Set).
   Called by mutators; carries `d` in metadata."
  [x accrue d]
  (vary-meta x update delta-key (fn [cur] (if (some? cur) (accrue cur d) d))))

(defn clear-delta
  "Return `x` with its local δ dropped — called once the δ has been propagated (or
   on a remote-integrated value, so it never re-propagates)."
  [x]
  (vary-meta x (fn [m] (not-empty (dissoc m delta-key)))))

(defn has-delta?
  "Whether `x` carries a non-empty local δ to propagate."
  [x]
  (let [d (delta-of x)]
    (and (some? d)
         (if (coll? d) (seq d) true))))
