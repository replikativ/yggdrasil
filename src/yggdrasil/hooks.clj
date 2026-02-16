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

(defmulti install-commit-hook!
  "Install an adapter-specific commit hook for auto-registration.

   on-commit-fn: (fn [{:type :commit :snapshot-id ... :branch ...}])

   Returns a hook-id for cleanup, or nil if the system doesn't
   support hooks. Dispatches on (system-type system)."
  (fn [_workspace system _on-commit-fn] (p/system-type system)))

(defmulti remove-commit-hook!
  "Remove a previously installed commit hook.
   hook-id: the value returned by install-commit-hook!.
   Dispatches on (system-type system)."
  (fn [_workspace system _hook-id] (p/system-type system)))
