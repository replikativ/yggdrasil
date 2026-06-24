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

(def default-opts
  "Default RUNTIME opts for a convergent op when the caller omits them. A system no
   longer carries its execution mode — `:sync?` is a per-call choice (so a JVM caller
   can run one op blocking and another async). When omitted it defaults SYNC on the JVM
   (blocking values) and ASYNC on cljs (no blocking primitive against IndexedDB). Pass
   an explicit `{:sync? …}` to override."
  #?(:clj {:sync? true} :cljs {:sync? false}))

(defprotocol PConvergent
  "A system whose value forms a join-semilattice — a CRDT."
  (-join [this other] [this other opts]
    "Least-upper-bound (merge) of this replica with peer `other`. Commutative,
     associative, idempotent. No ancestor, no conflicts, no parent/child. `opts`
     ({:sync?}, default `default-opts`) selects the sync/async return; the 2-arity
     uses the default.")
  (-conflict-free? [this]
    "True — marks this system as auto-converging (capability dispatch: the
     merge path skips ancestor lookup and never surfaces conflicts)."))

(defn join
  "Fold `-join` over peer replicas → the converged value. Order-independent.
   Uses `default-opts`; for a non-default mode call `-join` with explicit opts."
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
;; their op (`with-delta`). INTEGRATING A PEER yields a δ-FREE value: both
;; `-apply-delta` (op-path) and `-join`'s *changed* branch `clear-delta`, so the
;; signal layer's export-on-change watch ships NOTHING after an integration — the
;; receiver's own ops were already shipped at their mutation, and the peer's ops
;; must not echo. A *no-op* `-join` returns the receiver IDENTICAL (the signal
;; layer's `identical?`-skip suppresses its watch, so its residual δ is never
;; acted on — harmless). So the δ is only ever "this turn's local op, in flight".

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

(defprotocol PDeltaApply
  "Consume a peer's δ — the OP-path counterpart to `-join` (the STATE-path)."
  (-apply-delta [this delta] [this delta opts]
    "Integrate δ (a value produced by `delta-of` on a peer replica) into this
     replica's state — O(δ), no full -join, no diffing. Returns the new replica
     WITHOUT a local δ (remote-integrated ops do not re-propagate). `opts` ({:sync?},
     default `default-opts`) selects the sync/async return; the 2-arity uses the default."))
