(ns yggdrasil.convergent.overlay
  "`Overlayable` for convergent (CRDT) systems — the UNIFORM isolate primitive.

   `overlay` clones the system with fresh mutable state at its current value; you
   mutate the clone in isolation (its normal functional ops); `merge-down!` joins
   the clone back into the parent; `discard!` drops it. Because isolation is a
   clone (fresh atoms over the SAME content-addressed store — immutable nodes), it
   works for ANY convergent system — G-Set, 2P-Set, OR-Set — so a CRDT isolates
   WITHOUT needing the named-branch model. This is what resolves the 2P/OR-Set
   `branch!`-no-op residue.

   `branch!` (Branchable) = a durable NAMED ref (git/datahike model). `overlay`
   (this) = a transient ISOLATED WORKSPACE with a fork→merge-down/discard
   lifecycle (the spindel-fork model). Complementary, not redundant.

   `mode :frozen` (isolate at the current value) is the proven path. The
   live-tracking modes `:following`/`:gated` are thin here — `advance!` re-joins
   the parent's current state into the overlay; the sequence-lock/gated machinery
   is deferred until an agent-tracking case needs it."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.convergent :as c]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(defn overlay-system
  "The Overlay's isolated, writable clone — mutate it with the parent system's
   normal functional ops (add/remove/…); `merge-down!` joins it back."
  [overlay]
  @(:local-writes overlay))

(defn convergent-overlay
  "Build an Overlay over a convergent `parent` system: a fresh-atoms clone of
   `parent` at its current value, isolated. `clone-fn` returns the isolated copy
   (per-record: fresh mutable atoms, same content). `base` is the parent's
   snapshot-id at creation (the observation point)."
  [parent mode base clone-fn]
  (t/->Overlay (str (random-uuid)) parent (or mode :frozen) base
               (atom (clone-fn parent)) (t/now-ms)))

;; The overlay-SIDE methods are generic — they only use the parent's convergent
;; `-join` and the cloned system held in `:local-writes`.
(extend-type #?(:clj yggdrasil.types.Overlay :cljs t/Overlay)
  p/Overlayable
  (base-ref [ov] (:base-snapshot ov))

  (peek-parent
    ([ov] (:parent ov))
    ([ov _opts] (:parent ov)))

  ;; the isolated clone embodies the overlay's writes — diff it against the
  ;; parent for the element-level delta.
  (overlay-writes [ov] @(:local-writes ov))

  ;; :following/:gated — re-join the parent's CURRENT state into the overlay so it
  ;; picks up the parent's advances (thin; no gated sequence-lock yet).
  (advance!
    ([ov] (p/advance! ov nil))
    ([ov _opts]
     (let [parent (:parent ov)]
       (async+sync (:sync? (:opts parent))
                   (async
                    (reset! (:local-writes ov) (await (c/-join @(:local-writes ov) parent)))
                    ov)))))

  ;; push the overlay's isolated state back into the parent (convergent join).
  ;; Returns the merged parent (value-semantic).
  (merge-down!
    ([ov] (p/merge-down! ov nil))
    ([ov _opts]
     (let [parent (:parent ov)]
       (async+sync (:sync? (:opts parent))
                   (async (await (c/-join parent @(:local-writes ov))))))))

  ;; abandon the overlay — its isolated nodes become orphans reclaimed by GC.
  (discard!
    ([ov] nil)
    ([ov _opts] nil)))
