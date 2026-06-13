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

(extend-type yggdrasil.composite.CompositeSystem
  c/PConvergent
  (-join [this other]
    (let [others (:systems other)
          joined (map (fn [[id sys]]
                        (let [o (get others id)]
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
                            :else sys)))
                      (:systems this))]
      (comp/composite (vec joined)
                      :name (:composite-name this)
                      :branch (:current-branch-name this))))
  (-conflict-free? [this]
    (every? c/convergent? (vals (:systems this)))))
