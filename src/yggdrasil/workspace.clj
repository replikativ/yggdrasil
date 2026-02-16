(ns yggdrasil.workspace
  "Multi-system workspace with HLC coordination and ref management.

   The Workspace is the top-level coordination layer. It:
   - Holds system refs across multiple backends
   - Coordinates HLC timestamps for multi-system commits
   - Manages the snapshot registry
   - Tracks held branch references for GC safety
   - Provides as-of-world queries across all systems

   Primary API: manage!/unmanage! — add systems with auto-registration
   of commits via adapter-specific hooks (datahike listen, etc.) or
   Watchable polling fallback.

   Branch management is delegated to each system's native mechanism:
   - Datahike: connection per branch (single writer semantics)
   - Git: worktree per branch
   - Btrfs: subvolume per branch
   - ZFS: dataset per branch"
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.registry :as reg]
            [yggdrasil.hooks :as hooks]))

;; ============================================================
;; Workspace record
;; ============================================================

(defrecord Workspace
           [registry       ; Registry — snapshot registry
            hlc-atom        ; atom of HLC — shared causal clock
            systems         ; atom of {system-id → system} — primary branch per system
            refs            ; atom of {ref-key → system} — held branch refs
            conn-cache      ; atom of {[system-id branch] → system} — reuse connections
            watchers])      ; atom of {system-id → watch-id} — active watchers

;; ============================================================
;; Default hook implementations (Watchable fallback)
;; ============================================================

(defmethod hooks/install-commit-hook! :default
  [workspace system on-commit-fn]
  (when (satisfies? p/Watchable system)
    (p/watch! system
              (fn [event]
                (when (= :commit (:type event))
                  (on-commit-fn event)))
              {})))

(defmethod hooks/remove-commit-hook! :default
  [workspace system hook-id]
  (when (satisfies? p/Watchable system)
    (p/unwatch! system hook-id)))

;; ============================================================
;; Factory
;; ============================================================

(defn create-workspace
  "Create a workspace with optional persistent registry.

   opts:
     :store-path  - path for registry persistence (nil for in-memory only)"
  ([] (create-workspace {}))
  ([opts]
   (->Workspace (reg/create-registry opts)
                (atom (t/hlc-now))
                (atom {})
                (atom {})
                (atom {})
                (atom {}))))

;; ============================================================
;; System management
;; ============================================================

(defn add-system!
  "Add a system to the workspace. Registers its current snapshot."
  [workspace system]
  (let [sid (p/system-id system)]
    (swap! (:systems workspace) assoc sid system)
    ;; Register current state
    (when (satisfies? p/Snapshotable system)
      (when-let [snap-id (p/snapshot-id system)]
        (reg/register! (:registry workspace)
                       (t/->RegistryEntry
                        (str snap-id)
                        sid
                        (if (satisfies? p/Branchable system)
                          (name (p/current-branch system))
                          "main")
                        @(:hlc-atom workspace)
                        nil
                        (when (satisfies? p/Snapshotable system)
                          (p/parent-ids system))
                        {:source :add-system}))))
    workspace))

(defn remove-system!
  "Remove a system from the workspace.
   Cleans up any installed hooks (native or Watchable)."
  [workspace system-id]
  ;; Clean up hooks via the hooks multimethod (handles native + Watchable)
  (when-let [hook-id (get @(:watchers workspace) system-id)]
    (when-let [sys (get @(:systems workspace) system-id)]
      (try (hooks/remove-commit-hook! workspace sys hook-id)
           (catch Exception _))))
  (swap! (:watchers workspace) dissoc system-id)
  (swap! (:systems workspace) dissoc system-id)
  ;; Clean up connection cache entries for this system
  (swap! (:conn-cache workspace)
         (fn [cache]
           (into {} (remove (fn [[[sid _] _]] (= sid system-id))) cache)))
  workspace)

(defn get-system
  "Get a system by its system-id."
  [workspace system-id]
  (get @(:systems workspace) system-id))

(defn list-systems
  "List all system-ids in the workspace."
  [workspace]
  (keys @(:systems workspace)))

;; ============================================================
;; HLC coordination
;; ============================================================

(defn tick!
  "Tick the workspace HLC and return the new value.
   Used to pin a timestamp for coordinated multi-system commits."
  [workspace]
  (swap! (:hlc-atom workspace) t/hlc-tick))

(defn current-hlc
  "Return the current HLC value without ticking."
  [workspace]
  @(:hlc-atom workspace))

(defn receive-hlc!
  "Update the workspace HLC with a remote HLC (on message receive)."
  [workspace remote-hlc]
  (swap! (:hlc-atom workspace) t/hlc-receive remote-hlc))

;; ============================================================
;; Coordinated commits
;; ============================================================

(defn begin-transaction!
  "Begin a coordinated transaction. Ticks the HLC and returns
   the pinned timestamp. All commits in this transaction should
   use the returned HLC for registry."
  [workspace]
  (tick! workspace))

(defn commit-with-hlc!
  "Commit on a system and register with a pinned HLC.

   commit-fn: (fn [system] -> commit-id) — performs the actual commit
              using the adapter's native API.

   Returns the RegistryEntry created for this commit."
  [workspace system-id pinned-hlc commit-fn]
  (let [system (get @(:systems workspace) system-id)]
    (when-not system
      (throw (ex-info "System not found in workspace"
                      {:system-id system-id
                       :available (vec (keys @(:systems workspace)))})))
    (let [branch-name (if (satisfies? p/Branchable system)
                        (name (p/current-branch system))
                        "main")
          parent-ids (when (satisfies? p/Snapshotable system)
                       (p/parent-ids system))
          commit-id (commit-fn system)
          entry (t/->RegistryEntry
                 (str commit-id)
                 system-id
                 branch-name
                 pinned-hlc
                 nil
                 parent-ids
                 nil)]
      (reg/register! (:registry workspace) entry)
      entry)))

(defn coordinated-commit!
  "Commit across multiple systems with the same pinned HLC.

   commit-fns: {system-id → (fn [system] -> commit-id)}

   Returns {:results {system-id → RegistryEntry}
            :errors  {system-id → Exception}
            :hlc     <pinned HLC>}

   Partial failures are captured per-system — successful commits are
   registered, failed ones are recorded in :errors. Callers should
   check :errors to detect and handle partial failures."
  [workspace commit-fns]
  (let [pinned-hlc (begin-transaction! workspace)
        results (atom {})
        errors (atom {})]
    (doseq [[system-id commit-fn] commit-fns]
      (try
        (let [entry (commit-with-hlc! workspace system-id pinned-hlc commit-fn)]
          (swap! results assoc system-id entry))
        (catch Exception e
          (swap! errors assoc system-id e))))
    {:results @results
     :errors @errors
     :hlc pinned-hlc}))

;; ============================================================
;; Branch ref management
;; ============================================================

(defn hold-ref!
  "Hold a reference to a system at a specific branch.
   This prevents GC from collecting the branch's snapshots.

   ref-key: string like 'git:repo1/:feature'
   system: the system value (already checked out to the branch)"
  [workspace ref-key system]
  (swap! (:refs workspace) assoc ref-key system)
  ;; Cache the connection for reuse
  (let [sid (p/system-id system)
        branch (if (satisfies? p/Branchable system)
                 (name (p/current-branch system))
                 "main")]
    (swap! (:conn-cache workspace) assoc [sid branch] system)
    ;; Register in the registry
    (when (satisfies? p/Snapshotable system)
      (when-let [snap-id (p/snapshot-id system)]
        (reg/register! (:registry workspace)
                       (t/->RegistryEntry
                        (str snap-id)
                        sid
                        branch
                        @(:hlc-atom workspace)
                        nil
                        (p/parent-ids system)
                        {:held true :ref-key ref-key})))))
  workspace)

(defn release-ref!
  "Release a held reference. The branch's snapshots become
   eligible for GC after the grace period."
  [workspace ref-key]
  (when-let [system (get @(:refs workspace) ref-key)]
    (let [sid (p/system-id system)
          branch (if (satisfies? p/Branchable system)
                   (name (p/current-branch system))
                   "main")]
      (swap! (:conn-cache workspace) dissoc [sid branch])))
  (swap! (:refs workspace) dissoc ref-key)
  workspace)

(defn held-refs
  "List all currently held references."
  [workspace]
  @(:refs workspace))

(defn get-cached-connection
  "Get a cached connection/system for a system-id and branch.
   Returns nil if not cached."
  [workspace system-id branch]
  (get @(:conn-cache workspace) [system-id branch]))

;; ============================================================
;; Internal: Watchable polling (used by manage! as fallback)
;; ============================================================

(defn- stop-hook!
  "Stop a hook for a system. Used internally by close!."
  [workspace system-id]
  (when-let [hook-id (get @(:watchers workspace) system-id)]
    (when-let [system (get @(:systems workspace) system-id)]
      (try (hooks/remove-commit-hook! workspace system hook-id)
           (catch Exception _))))
  (swap! (:watchers workspace) dissoc system-id))

;; ============================================================
;; Managed systems — high-level API
;; ============================================================

(defn manage!
  "Add a system with auto-registration of commits.

   This is the recommended high-level API for adding systems to a
   workspace. It sets up adapter-specific commit hooks so that every
   commit is automatically registered in the snapshot registry.

   Hook mechanisms (in priority order):
   - Native hooks via hooks/install-commit-hook! multimethod
     (e.g., datahike uses d/listen for immediate notification)
   - Watchable polling fallback for adapters without native hooks

   Adapters extend yggdrasil.hooks/install-commit-hook! for their
   system type to provide optimal hook behavior.

   Returns the workspace."
  ([workspace system] (manage! workspace system {}))
  ([workspace system opts]
   (let [sid (p/system-id system)]
     ;; Add system to workspace (registers current snapshot)
     (add-system! workspace system)
     ;; Set up commit hook for auto-registration
     (let [on-commit (fn [event]
                       (let [hlc (swap! (:hlc-atom workspace) t/hlc-tick)]
                         (reg/register!
                          (:registry workspace)
                          (t/->RegistryEntry
                           (str (:snapshot-id event))
                           sid
                           (or (:branch event) "main")
                           hlc
                           nil
                           nil
                           {:source :managed-hook}))))
           hook-id (hooks/install-commit-hook! workspace system on-commit)]
       (when hook-id
         (swap! (:watchers workspace) assoc sid hook-id)))
     workspace)))

(defn unmanage!
  "Remove a managed system and its commit hooks.

   Cleans up any installed hooks (native or Watchable) and removes
   the system from the workspace."
  [workspace system-id]
  (when-let [system (get @(:systems workspace) system-id)]
    (when-let [hook-id (get @(:watchers workspace) system-id)]
      (try
        (hooks/remove-commit-hook! workspace system hook-id)
        (catch Exception _))))
  (remove-system! workspace system-id))

;; ============================================================
;; Temporal queries
;; ============================================================

(defn as-of-world
  "Query the state of all systems at time T.
   Returns {[system-id branch-name] → RegistryEntry}."
  [workspace hlc]
  (reg/as-of (:registry workspace) hlc))

(defn as-of-time
  "Query world state at a wall-clock time (millis since epoch).
   Uses HLC-ceil to capture all events at or before that physical time,
   including logical ticks within the same millisecond."
  [workspace physical-ms]
  (reg/as-of (:registry workspace) (t/->HLC-ceil physical-ms)))

;; ============================================================
;; Registry sync — populate from existing system state
;; ============================================================

(defn sync-registry!
  "Scan a system's actual state and add missing entries to the registry.
   Useful for initial population or catch-up after external changes.

   Only works with Graphable + Branchable systems."
  [workspace system-id]
  (let [system (get @(:systems workspace) system-id)]
    (when-not system
      (throw (ex-info "System not found" {:system-id system-id})))
    (when (and (satisfies? p/Branchable system)
               (satisfies? p/Graphable system))
      (doseq [branch (p/branches system)]
        (let [checked-out (p/checkout system branch)
              branch-name (name branch)
              history-ids (p/history checked-out)]
          (doseq [snap-id history-ids]
            (let [existing (reg/snapshot-refs (:registry workspace) (str snap-id))]
              ;; Only register if not already tracked
              (when-not (some #(and (= (:system-id %) system-id)
                                    (= (:branch-name %) branch-name))
                              existing)
                (let [meta (when (satisfies? p/Snapshotable checked-out)
                             (p/snapshot-meta checked-out snap-id))
                      hlc (if-let [ts (:timestamp meta)]
                            (t/->HLC (try (long (Double/parseDouble (str ts)))
                                          (catch Exception _
                                            (System/currentTimeMillis)))
                                     0)
                            (swap! (:hlc-atom workspace) t/hlc-tick))]
                  (reg/register!
                   (:registry workspace)
                   (t/->RegistryEntry
                    (str snap-id)
                    system-id
                    branch-name
                    hlc
                    nil
                    (:parent-ids meta)
                    (merge {:source :sync}
                           (select-keys meta [:message :author]))))))))))))
  workspace)

;; ============================================================
;; Garbage collection
;; ============================================================

(defn gc!
  "Run coordinated garbage collection across all managed systems.

   Delegates to yggdrasil.gc/gc-sweep! with the workspace's registry
   and currently managed systems.

   opts:
     :grace-period-ms       - minimum age before collection (default 7 days)
     :dry-run?              - if true, return report without deleting
     :freed-grace-period-ms - grace period for freed B-tree nodes (default 1 hour)

   Returns {:swept [...] :errors {...} :freed-nodes-swept count}"
  ([workspace] (gc! workspace {}))
  ([workspace opts]
   (let [gc-sweep! (requiring-resolve 'yggdrasil.gc/gc-sweep!)]
     (gc-sweep! (:registry workspace)
                (vals @(:systems workspace))
                opts))))

;; ============================================================
;; Lifecycle
;; ============================================================

(defn close!
  "Close the workspace, flush registry, stop all hooks."
  [workspace]
  ;; Stop all hooks
  (doseq [[system-id _] @(:watchers workspace)]
    (stop-hook! workspace system-id))
  ;; Flush and close registry
  (reg/close! (:registry workspace))
  nil)
