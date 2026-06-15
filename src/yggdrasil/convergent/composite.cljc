(ns yggdrasil.convergent.composite
  "Make a CompositeSystem itself conflict-free-mergeable: joining two PEER
   workspaces fans `-join` out to matching sub-systems. This is \"merge peers\"
   with NO new interface and NO parent — merging two contexts is merging their
   two workspace composites, which is a *system* merge via the existing
   `PConvergent`/`-join`. `merge-to-parent!` is just the special case
   target=parent,source=child; the symmetric peer case is the same op.

   Per sub-system dispatch:
   - convergent (CRDT) sub-system → `-join` (symmetric, no ancestor)
   - versioned (datahike/git) sub-system → 3-way merge (needs the shared
     ancestor) — belongs here too, wired in L4 (currently errors if hit)."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.composite :as comp]))

;; The composite is a STATE container (the atomic-workspace value). It syncs via
;; the STATE path — konserve-sync ships its nodes + the `:composite/root` causal
;; gate, and `-join` reconciles two whole workspaces. It deliberately has NO
;; op-δ: a δ is the change a LOCAL mutation made, but a composite isn't mutated
;; directly — you mutate a SUB, which returns a new (δ-carrying) record the
;; composite doesn't capture. The OP path is therefore per-LEAF: sync each
;; sub-system as its own ygg-signal (the dissolution model). Forcing an aggregate
;; δ onto the composite would be a duplication against the grain.
;;
;; cljc: the only platform-specific token is the record TYPE symbol (a Java class
;; on JVM, the cljs var on cljs) — mirrors overlay.cljc's `extend-type` over the
;; reader-conditional type. Everything else is plain cross-platform protocol code,
;; so a browser peer can `-join` two composite workspaces too.
(extend-type #?(:clj yggdrasil.composite.CompositeSystem :cljs comp/CompositeSystem)
  c/PConvergent
  (-join [this other]
    (let [others (:systems other)
          joined (reduce
                  (fn [m [id sys]]
                    (let [o (get others id)]
                      (assoc m id
                             (cond
                               (nil? o) sys                       ; system only on `this`
                               (c/convergent? sys) (c/-join sys o) ; CRDT: symmetric 2-way join
                               ;; versioned (datahike/git): 3-way merge — merge `other`'s
                               ;; branch into `this`'s on the shared store. This IS the
                               ;; per-system logic merge-to-parent! reimplements; conflicts
                               ;; are surfaced via the composite's `conflicts` (the
                               ;; merge-review seam), not auto-resolved here. Requires a
                               ;; resolvable common ancestor (shared store / forked peer).
                               (satisfies? p/Mergeable sys)
                               (-> sys
                                   (p/checkout (p/current-branch sys))
                                   (p/merge! (p/current-branch o)))
                               :else sys))))
                  {} (:systems this))]
      ;; IDEMPOTENCE: when every sub-join changed nothing (each returns the SAME
      ;; sub), the composite is unchanged → return `this` identical, so a
      ;; composite-valued signal doesn't re-fire / re-publish on a no-op.
      (if (= joined (:systems this))
        this
        (comp/composite (vec (vals joined))
                        :name (:composite-name this)
                        :branch (:current-branch-name this)))))
  (-conflict-free? [this]
    (every? c/convergent? (vals (:systems this)))))
