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
