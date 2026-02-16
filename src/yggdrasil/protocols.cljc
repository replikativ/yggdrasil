(ns yggdrasil.protocols
  "Unified Copy-on-Write memory model protocols.

  All protocols use VALUE SEMANTICS: mutating operations return a new
  system value rather than modifying in place. This aligns with Clojure
  idioms and supports threading:

    (-> (create-system config)
        (branch! :feature)
        (checkout :feature)
        ...)

  Seven layers, each optional:
    1. Snapshotable        - point-in-time immutable snapshots
    2. Branchable          - named references (value-semantic)
    3. Graphable           - history/DAG traversal
    4. Mergeable           - combine lineages (value-semantic)
    5. Overlayable         - live fork with observation modes
    6. Watchable           - state change observation
    7. GarbageCollectable  - coordinated cross-system GC"
  (:refer-clojure :exclude [ancestors]))

;; ============================================================
;; Layer 1: Snapshotable (fundamental)
;; ============================================================

(defprotocol Snapshotable
  "Point-in-time immutable snapshots. Every CoW system implements this."

  (snapshot-id [this]
    "Current snapshot ID. Returns UUID or content-hash string.")

  (parent-ids [this]
    "Parent snapshot IDs of current state. Returns set of snapshot-id strings.")

  (as-of [this snap-id] [this snap-id opts]
    "Read-only view at given snapshot. Returns system-specific read view.
     opts: {:sync? true} — when false, returns channel/promise.")

  (snapshot-meta [this snap-id] [this snap-id opts]
    "Metadata for snapshot. Returns map with :timestamp, :author, :message, etc.
     opts: {:sync? true} — when false, returns channel/promise."))

;; ============================================================
;; Layer 2: Branchable (named references)
;; ============================================================

(defprotocol Branchable
  "Named references to snapshots. Value-semantic: operations return new system."

  (branches [this] [this opts]
    "List all branch names. Returns set of keywords.
     opts: {:sync? true} — when false, returns channel/promise.")

  (current-branch [this]
    "Current branch name. Returns keyword.")

  (branch! [this name] [this name from] [this name from opts]
    "Create branch from current state (or `from` snapshot-id/branch).
     Returns new system with branch created. Current branch unchanged.
     opts: {:sync? true} — when false, returns channel/promise.")

  (delete-branch! [this name] [this name opts]
    "Remove branch. Returns new system without the branch.
     Underlying data remains until GC.
     opts: {:sync? true} — when false, returns channel/promise.")

  (checkout [this name] [this name opts]
    "Switch to branch. Returns new system at branch head.
     opts: {:sync? true} — when false, returns channel/promise."))

;; ============================================================
;; Layer 3: Graphable (history/DAG traversal)
;; ============================================================

(defprotocol Graphable
  "DAG traversal and history. For systems with rich commit graphs."

  (history [this] [this opts]
    "Commit history as seq of snapshot-ids, newest first.
     opts: {:limit n, :since snap-id, :sync? true}")

  (ancestors [this snap-id] [this snap-id opts]
    "All ancestor snapshot-ids of given snapshot.
     opts: {:sync? true} — when false, returns channel/promise.")

  (ancestor? [this a b] [this a b opts]
    "True if snapshot a is an ancestor of snapshot b.
     opts: {:sync? true} — when false, returns channel/promise.")

  (common-ancestor [this a b] [this a b opts]
    "Merge base: most recent common ancestor of a and b.
     Returns snapshot-id or nil if unrelated.
     opts: {:sync? true} — when false, returns channel/promise.")

  (commit-graph [this] [this opts]
    "Full DAG structure.
     Returns {:nodes {id {:parent-ids #{...} :meta {...}}}
              :branches {:main id ...}
              :roots #{...}}
     opts: {:sync? true} — when false, returns channel/promise.")

  (commit-info [this snap-id] [this snap-id opts]
    "Metadata for specific commit.
     Returns {:parent-ids, :timestamp, :author, :message, ...}
     opts: {:sync? true} — when false, returns channel/promise."))

;; ============================================================
;; Layer 4: Mergeable (combine lineages)
;; ============================================================

(defprotocol Mergeable
  "Merge support. Value-semantic: merge! returns new system."

  (merge! [this source] [this source opts]
    "Merge source branch/snapshot into current.
     opts: {:strategy :ours|:theirs|:union|fn
            :message \"merge commit message\"
            :sync? true}
     Returns new system with merge applied.
     Use (snapshot-id result) to get the merge commit ID.")

  (conflicts [this a b] [this a b opts]
    "Detect conflicts between two snapshots without merging.
     Returns seq of conflict descriptors.
     opts: {:sync? true} — when false, returns channel/promise.")

  (diff [this a b] [this a b opts]
    "Compute delta between two snapshots.
     Returns system-specific delta representation.
     opts: {:sync? true} — when false, returns channel/promise."))

;; ============================================================
;; Layer 5: Overlayable (live fork - Spindel integration)
;; ============================================================

(defprotocol Overlayable
  "Live fork that can observe parent's evolution.
   Three modes: :frozen, :following, :gated."

  (overlay [this opts]
    "Create overlay on top of system.
     opts: {:mode :frozen|:following|:gated, :sync? true}
     Returns Overlay.")

  (advance! [overlay] [overlay opts]
    "Sync overlay to parent's current state (gated mode).
     Uses sequence lock pattern for atomic observation.
     opts: {:sync? true} — when false, returns channel/promise.")

  (peek-parent [overlay] [overlay opts]
    "Read parent's current state without advancing overlay's base-ref.
     Returns read-only view of parent's latest state.
     opts: {:sync? true} — when false, returns channel/promise.")

  (base-ref [overlay]
    "SnapshotRef of overlay's current observation point.")

  (overlay-writes [overlay]
    "Delta of overlay's isolated writes since creation/last-advance.")

  (merge-down! [overlay] [overlay opts]
    "Push overlay writes to parent. May fail on conflict.
     opts: {:sync? true} — when false, returns channel/promise.")

  (discard! [overlay] [overlay opts]
    "Abandon overlay and all its isolated writes.
     opts: {:sync? true} — when false, returns channel/promise."))

;; ============================================================
;; Layer 6: Watchable (state change observation)
;; ============================================================

(defprotocol Watchable
  "Observe state changes via polling or event notification.
   Supports both orchestrator mode (yggdrasil drives changes)
   and observer mode (external changes detected automatically)."

  (watch! [this callback] [this callback opts]
    "Register callback for state change events. Returns watch-id string.
     callback: (fn [{:type :commit|:branch-created|:branch-deleted|:checkout
                     :snapshot-id \"...\"
                     :branch \"...\"
                     :timestamp <millis>}])
     opts: {:poll-interval-ms 1000, :sync? true}")

  (unwatch! [this watch-id] [this watch-id opts]
    "Stop watching. Removes callback and cleans up if last watcher.
     opts: {:sync? true} — when false, returns channel/promise."))

;; ============================================================
;; Layer 7: GarbageCollectable (optional, coordinated GC)
;; ============================================================

(defprotocol GarbageCollectable
  "Coordinated garbage collection support. Systems implement this
   to participate in cross-system GC via the registry.

   The GC coordinator calls gc-roots to discover live references,
   then gc-sweep! to delete unreachable snapshots. Each system
   performs deletion using its native mechanism."

  (gc-roots [this]
    "Return set of snapshot-ids that are live roots (branch heads, etc.).
     These snapshots and their ancestors will not be collected.")

  (gc-sweep! [this snapshot-ids] [this snapshot-ids opts]
    "Delete the given snapshots from the system's native storage.
     Only deletes snapshots that are safe to remove per the system's logic.
     Returns new system with snapshots removed.
     opts: {:sync? true} — when false, returns channel/promise."))

;; ============================================================
;; System identity
;; ============================================================

(defprotocol SystemIdentity
  "System identification and capability advertisement."

  (system-id [this]
    "Unique identifier for this system instance. Returns string.")

  (system-type [this]
    "Type keyword: :datahike, :proximum, :git, :zfs, :docker, :repl, etc.")

  (capabilities [this]
    "Map of supported protocols.
     {:snapshotable true
      :branchable true
      :graphable true
      :mergeable false
      :overlayable false}"))
