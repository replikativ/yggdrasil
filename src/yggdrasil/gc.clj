(ns yggdrasil.gc
  "Coordinated garbage collection across heterogeneous systems.

   GC follows a two-tier pattern inspired by Datahike:

   Tier 1 (ref-counting): The registry tracks all snapshot references.
   Each register/deregister adjusts visibility. No actual deletion.

   Tier 2 (mark-and-sweep with grace period):
   1. Mark: Walk all systems' branch heads → compute reachable set
   2. Retain: Keep snapshots within retention window (configurable)
   3. Sweep: For unreachable snapshots beyond grace period,
             delegate deletion to each adapter's native GC
   4. Conservative on failure: If any system is unreachable,
             skip GC for all its snapshots

   Each system runs its own internal GC (Btrfs prunes snapshots,
   Git runs git gc, etc.) but coordinates externally via the registry
   to prevent cross-system reference breakage."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.registry :as reg]
            [yggdrasil.storage :as store]
            [yggdrasil.types :as t]))

;; ============================================================
;; Reachability analysis
;; ============================================================

(defn compute-reachable-set
  "Walk all systems' branch heads and compute the set of reachable
   snapshot-ids. A snapshot is reachable if it is a branch head or
   an ancestor of a branch head.

   Returns #{snapshot-id}."
  [systems]
  (let [reachable (atom #{})]
    (doseq [sys systems]
      (when (and (satisfies? p/Branchable sys)
                 (satisfies? p/Snapshotable sys))
        ;; Add the current branch head
        (when-let [head (p/snapshot-id sys)]
          (swap! reachable conj head))
        ;; Walk all branches
        (doseq [branch (p/branches sys)]
          (try
            (let [checked-out (p/checkout sys branch)
                  head (p/snapshot-id checked-out)]
              (when head
                (swap! reachable conj head)
                ;; Walk ancestors if Graphable
                (when (satisfies? p/Graphable checked-out)
                  (when-let [ancs (seq (p/ancestors checked-out head))]
                    (swap! reachable into ancs)))))
            (catch Exception _
              ;; Skip branches we can't checkout
              nil)))))
    @reachable))

(defn compute-gc-roots
  "Collect gc-roots from all GarbageCollectable systems.
   Falls back to branch heads for non-GarbageCollectable systems.
   Returns #{snapshot-id}."
  [systems]
  (let [roots (atom #{})]
    (doseq [sys systems]
      (if (satisfies? p/GarbageCollectable sys)
        (swap! roots into (p/gc-roots sys))
        ;; Fallback: branch heads
        (when (satisfies? p/Snapshotable sys)
          (when-let [head (p/snapshot-id sys)]
            (swap! roots conj head)))))
    @roots))

;; ============================================================
;; GC candidate selection
;; ============================================================

(defn gc-candidates
  "Find registry entries for snapshots that are:
   1. Not in the reachable set
   2. Older than the grace period

   Returns seq of RegistryEntry."
  [registry reachable-set grace-period-ms]
  (let [now (System/currentTimeMillis)
        cutoff-hlc (t/->HLC (- now grace-period-ms) 0)
        all (reg/all-entries registry)]
    (->> all
         (remove #(contains? reachable-set (:snapshot-id %)))
         (filter #(neg? (t/hlc-compare (:hlc %) cutoff-hlc)))
         vec)))

;; ============================================================
;; GC execution
;; ============================================================

(defn gc-sweep!
  "Perform coordinated garbage collection.

   1. Compute reachable set from all systems
   2. Find candidates (unreachable + beyond grace period)
   3. Delegate to each system's GarbageCollectable.gc-sweep!
   4. Only deregister after adapter confirms successful deletion
   5. Sweep freed PSS B-tree nodes from registry storage

   opts:
     :grace-period-ms       - minimum age before collection (default 7 days)
     :dry-run?              - if true, return report without deleting
     :freed-grace-period-ms - grace period for freed B-tree nodes (default 1 hour)

   Returns {:swept [RegistryEntry ...], :errors {system-id Exception ...},
            :freed-nodes-swept count}"
  ([registry systems] (gc-sweep! registry systems {}))
  ([registry systems opts]
   (let [grace-ms (or (:grace-period-ms opts)
                      (* 7 24 60 60 1000))
         reachable (compute-reachable-set systems)
         candidates (gc-candidates registry reachable grace-ms)]
     (if (:dry-run? opts)
       {:swept [] :candidates candidates :reachable reachable
        :errors {} :freed-nodes-swept 0}
       (let [by-system (group-by :system-id candidates)
             swept (atom [])
             errors (atom {})]
         ;; For each system, delegate deletion then deregister
         (doseq [[system-id entries] by-system]
           (let [sys (first (filter #(= (p/system-id %) system-id) systems))]
             (if (and sys (satisfies? p/GarbageCollectable sys))
               (let [snap-ids (set (map :snapshot-id entries))]
                 (try
                   ;; Step 1: adapter deletes native snapshots
                   (p/gc-sweep! sys snap-ids)
                   ;; Step 2: only deregister after adapter confirms success
                   (doseq [entry entries]
                     (reg/deregister! registry entry))
                   (swap! swept into entries)
                   (catch Exception e
                     ;; Conservative: skip on failure — entries stay in registry
                     (swap! errors assoc system-id e))))
               ;; System not GarbageCollectable — skip
               nil)))
         ;; Flush registry changes
         (reg/flush! registry)
         ;; Sweep freed PSS B-tree nodes from storage
         (let [freed-count
               (if-let [storage (:storage registry)]
                 (let [freed-grace (or (:freed-grace-period-ms opts)
                                       (* 60 60 1000))]
                   (store/sweep-freed! (:kv-store registry)
                                       (:freed-atom storage)
                                       freed-grace))
                 0)]
           {:swept @swept
            :errors @errors
            :freed-nodes-swept freed-count}))))))

;; ============================================================
;; GC reporting
;; ============================================================

(defn gc-report
  "Generate a GC report without performing any deletions.

   Returns:
     {:reachable    #{snapshot-id}
      :candidates   [RegistryEntry ...]
      :by-system    {system-id [RegistryEntry ...]}
      :total-entries count
      :gc-eligible   count}"
  ([registry systems] (gc-report registry systems {}))
  ([registry systems opts]
   (let [grace-ms (or (:grace-period-ms opts)
                      (* 7 24 60 60 1000))
         reachable (compute-reachable-set systems)
         candidates (gc-candidates registry reachable grace-ms)]
     {:reachable reachable
      :candidates candidates
      :by-system (group-by :system-id candidates)
      :total-entries (reg/entry-count registry)
      :gc-eligible (count candidates)})))
