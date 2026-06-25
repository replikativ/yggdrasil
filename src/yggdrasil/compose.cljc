(ns yggdrasil.compose
  "Coordination primitives for multi-system operations.

  Yggdrasil does NOT prescribe ordering policy — that belongs to
  the orchestrating runtime (e.g., Spindel). This namespace provides
  mechanical helpers for overlay lifecycle across systems."
  (:require [yggdrasil.protocols :as p]))

;; ============================================================
;; Multi-system overlay lifecycle
;; ============================================================

(defn prepare-all
  "Create overlays on multiple Overlayable systems.
   Returns map of system-id → overlay.

   opts: {:mode :frozen|:following|:gated}  (default :gated)"
  ([systems] (prepare-all systems {:mode :gated}))
  ([systems opts]
   (into {}
         (map (fn [sys]
                [(p/system-id sys)
                 (p/overlay sys opts)])
              systems))))

(defn commit-seq!
  "Merge-down overlays in the given order.
   Stops on first failure, discards all remaining overlays.

   Returns:
     {:committed [overlay ...]   ; successfully merged
      :failed    overlay-or-nil  ; the one that failed (nil if all succeeded)
      :discarded [overlay ...]   ; abandoned after failure
      :error     exception-or-nil}"
  [overlays]
  (loop [remaining overlays
         committed []]
    (if (empty? remaining)
      {:committed committed :failed nil :discarded [] :error nil}
      (let [[current & rest] remaining
            ;; merge-down OUTSIDE the recur — `recur` cannot cross a `try`. Capture the
            ;; outcome, then either recur (success) or roll back the remainder (failure).
            outcome (try
                      (p/merge-down! current)
                      nil
                      (catch #?(:clj Exception :cljs :default) e e))]
        (if (nil? outcome)
          (recur rest (conj committed current))
          (do
            ;; discard all remaining overlays (best-effort)
            (doseq [o rest]
              (try (p/discard! o)
                   (catch #?(:clj Exception :cljs :default) _)))
            {:committed committed
             :failed current
             :discarded (vec rest)
             :error outcome}))))))

(defn discard-all!
  "Discard all overlays. Used for cleanup on abort."
  [overlays]
  (doseq [o overlays]
    (try (p/discard! o)
         (catch #?(:clj Exception :cljs :default) _))))

(defn snapshot-refs
  "Collect current SnapshotRefs from multiple systems.
   Useful for recording a cross-system checkpoint.

   For registry integration, use workspace/manage! or
   workspace/coordinated-commit! instead."
  [systems]
  (into {}
        (map (fn [sys]
               [(p/system-id sys)
                {:snapshot-id (p/snapshot-id sys)
                 :system-type (p/system-type sys)}])
             systems)))
