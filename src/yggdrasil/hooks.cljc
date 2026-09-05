(ns yggdrasil.hooks
  "Extension point for adapter-specific commit hooks.

   Each adapter can extend the install-commit-hook! multimethod to
   provide the most efficient auto-registration mechanism for its
   system type. For example:
   - Datahike uses d/listen for immediate, synchronous notification
   - Git falls back to Watchable polling

   The workspace's manage! function dispatches through these
   multimethods, so users get optimal hook behavior automatically."
  (:require [yggdrasil.protocols :as p]))

(defn normalize-commit-event
  "Complete an adapter notification into Yggdrasil's versioned-DAG event.

   Adapters should supply the exact `:snapshot-id`, `:parent-ids`, and `:branch`
   observed at their durable commit boundary. The fallbacks keep older
   Watchable adapters source-compatible, but reading parents from the live
   system can race a later commit and should not be used by new adapters.

   `:ordering` is deliberately opaque. A coordinator such as Urd may attach a
   fenced term/index later without making Yggdrasil depend on a consensus
   implementation."
  [system event]
  (let [snapshot-id (some-> (or (:snapshot-id event)
                                (p/snapshot-id system))
                            str)
        branch (or (:branch event)
                   (when (satisfies? p/Branchable system)
                     (some-> (p/current-branch system) name)))
        parent-ids (or (:parent-ids event)
                       (when (satisfies? p/Snapshotable system)
                         (p/parent-ids system))
                       #{})]
    (when-not snapshot-id
      (throw (ex-info "A commit event requires a snapshot ID"
                      {:type :yggdrasil/invalid-commit-event
                       :event event :system-id (p/system-id system)})))
    (when-not branch
      (throw (ex-info "A commit event requires a branch/ref"
                      {:type :yggdrasil/invalid-commit-event
                       :event event :system-id (p/system-id system)})))
    (-> event
        (assoc :type :commit
               :system-id (p/system-id system)
               :system-type (p/system-type system)
               :snapshot-id snapshot-id
               :parent-ids (set (map str parent-ids))
               :branch branch
               :ref (or (:ref event) branch)
               :durable? (get event :durable? true))
        (assoc :observed-at (or (:observed-at event)
                                (:timestamp event)
                                #?(:clj (System/currentTimeMillis)
                                   :cljs (js/Date.now)))))))

(defmulti install-commit-hook!
  "Install an adapter-specific commit hook for auto-registration.

   `on-commit-fn` receives a self-contained versioned-DAG event:

     {:type :commit
      :system-id string
      :system-type keyword
      :ref ref-name
      :branch branch-name
      :snapshot-id string
      :parent-ids #{string ...}
      :durable? true
      :observed-at epoch-millis
      :ordering optional-opaque-map}

   Implementations must report the parents belonging to that exact snapshot;
   consumers must not have to reread a mutable head. Delivery is at-least-once,
   so `(system-id, ref, snapshot-id)` is the stable deduplication identity.

   Returns a hook-id for cleanup, or nil if the system doesn't
   support hooks. Dispatches on (system-type system)."
  (fn [_workspace system _on-commit-fn] (p/system-type system)))

(defmulti remove-commit-hook!
  "Remove a previously installed commit hook.
   hook-id: the value returned by install-commit-hook!.
   Dispatches on (system-type system)."
  (fn [_workspace system _hook-id] (p/system-type system)))
