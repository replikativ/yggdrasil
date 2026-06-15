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

(defn overlay?
  "True if `x` is an Overlay (convergent `t/Overlay`, `DatahikeOverlay`, or
   `GitOverlay`) rather than a plain system. All three overlay records carry
   `:parent` + `:local-writes` + `:mode`; no system does — so this distinguishes a
   forked overlay value from a root system value (the spindel ygg-signal deref
   seam relies on it to unwrap)."
  [x]
  (and (record? x)
       (contains? x :local-writes)
       (contains? x :parent)
       (contains? x :mode)))

(defn overlay-system
  "The Overlay's isolated, WRITABLE system — mutate it with the parent's normal
   ops. For `:frozen` it's a clone (snapshot + own writes); for `:following` it's
   an empty DELTA holding only the overlay's own writes (read the effective value
   with `overlay-value`, which joins it with the live parent). Uniform across all
   overlay kinds (convergent / datahike / git) — each holds its writable system in
   the `:local-writes` atom."
  [overlay]
  @(:local-writes overlay))

(defn overlay-swap!
  "Apply value-semantic mutator `f` (system → new-system, possibly async) to the
   overlay's isolated writable system and RE-SEAT the result in `:local-writes`.
   Returns the overlay. (async+sync — `f` may return an async value.)

   Use this instead of mutating `(overlay-system ov)` in place: a value-semantic
   CRDT op returns a NEW system, so the overlay (a conn over `:local-writes`) must
   adopt it."
  [ov f]
  (let [parent (:parent ov)]
    (async+sync (:sync? (:opts parent))
                (async
                 (reset! (:local-writes ov) (await (f @(:local-writes ov))))
                 ov))))

(defn convergent-overlay
  "Build an Overlay over a convergent `parent`. `local-writes-system` is the
   isolated writable system the caller built for the mode:
     :frozen    a CLONE of the parent (snapshot-at-fork + own writes) — does NOT
                see the parent's later evolution.
     :following an EMPTY delta (own writes only); `overlay-value` joins it with
                the LIVE parent on read, so the overlay TRACKS the parent's
                concurrent evolution (the spindel OverlayBackend semantics)."
  [parent mode local-writes-system]
  (t/->Overlay (str (random-uuid)) parent (or mode :frozen) nil
               (atom local-writes-system) (t/now-ms)))

(defn overlay-value
  "The overlay's current EFFECTIVE system (read its value with the parent's
   normal ops, e.g. `elements`):
     :frozen    → the isolated clone (snapshot at fork + own writes).
     :following → the parent's LIVE state JOINED with the overlay's delta — so it
                  reflects the parent's concurrent evolution AND the overlay's own
                  writes (convergent, so the join can't conflict).
   (async+sync — the `:following` join is async on cljs.)"
  [ov]
  (let [parent (:parent ov)]
    (async+sync (:sync? (:opts parent))
                (async
                 (if (= :following (:mode ov))
                   (await (c/-join parent @(:local-writes ov)))
                   @(:local-writes ov))))))

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
