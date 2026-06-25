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
    7. GarbageCollectable  - coordinated cross-system GC

  ## Execution model: ONE portable codebase, sync on JVM / async on cljs

  Every IO-touching op takes `:sync?` in its `opts` and dispatches through the
  `async+sync` duality (`yggdrasil.macros`): with `:sync? true` it BLOCKS and returns
  a plain value; with `:sync? false` it returns a partial-cps CPS you `await`. The
  point is that you write your logic ONCE — `(await (snapshot-id sys opts))`, etc. —
  and it runs synchronously on the JVM and as non-blocking CPS on cljs, with no
  per-platform branching. (In the sync branch `async+sync` rewrites `await`→`do`, so a
  plain value flows straight through.)

  `:sync?` is a PER-CALL choice, not a property of the system (systems carry no
  execution mode). Omitted, it defaults to the PLATFORM default — `:sync? true` on the
  JVM, `:sync? false` on cljs. So the two natural regimes are **JVM-sync** and
  **cljs-async**, and portable code targets both for free.

  Fixed-arity reads with no `opts` slot (`snapshot-id`/`parent-ids`/`gc-roots`/
  `current-branch`) necessarily use the platform default; they are the reason a
  *mixed* JVM-async regime (`:sync? false` ON the JVM) is not a supported target — on
  the JVM those reads run sync and return a bare value, which a CPS caller cannot
  `await`. Use `:sync? false` on cljs (its platform default) or for explicitly
  async-konserve ops; do not force it on a JVM read path.

  VERSIONED ADAPTERS (git/datahike/dolt/iceberg/…) are JVM-only and inherently
  blocking (filesystem / JDBC / shelling out), so they are SYNC-ONLY: they accept
  `opts` for uniform protocol shape but ignore `:sync?` and always return plain
  values. This is sound because they only ever live where sync is the platform
  default (the JVM); they have no cljs incarnation, so the async branch never reaches
  them. Do NOT place a versioned adapter inside a `:sync? false` (async) composite —
  there is no async git/JDBC to dispatch to, and the bare value would break the
  caller's `await`. Only the convergent catalog + composite + storage layer are truly
  cross-platform."
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
  "Named references to snapshots.

   BRANCH IDENTITY (pinned contract): branch names are KEYWORDS everywhere — both as
   inputs (`branch!`/`checkout`/`delete-branch!`/`from`) and outputs
   (`current-branch`/`branches`). Pass keywords. Adapters MAY coerce a string with
   `keyword`, but callers should not rely on it. Default branch differs by adapter:
   CRDT/composite/git default `:main`; DATAHIKE has NO `:main` — its branch is the
   conn's `:config :branch`, conventionally `:db`. Do not hardcode `:main` against a
   datahike system (see `composite`/`pullback` for the mixed-default caveat).

   RETURN CONTRACT: value-semantic systems (CRDT, composite) return a NEW system value;
   `current-branch` is unchanged by `branch!`. STATEFUL adapters (datahike, git) are an
   exception: `branch!`/`delete-branch!` side-effect the shared conn/worktree and return
   `this` (the same record aliasing the now-mutated backend). `checkout` returns a fresh
   value on every adapter. Thread results functionally and do not assume `branch!` left
   the receiver untouched on stateful adapters."

  (branches [this] [this opts]
    "List all branch names. Returns set of KEYWORDS.
     opts: {:sync? true} — when false, returns channel/promise.")

  (current-branch [this]
    "Current branch name. Returns a KEYWORD.")

  (branch! [this name] [this name from] [this name from opts]
    "Create branch `name` (keyword) from current state (or `from` snapshot-id/branch).
     Value-semantic adapters return a new system with the branch created (current branch
     unchanged); stateful adapters (datahike/git) side-effect and return `this`.
     opts: {:sync? true} — when false, returns channel/promise.")

  (delete-branch! [this name] [this name opts]
    "Remove branch `name` (keyword). Returns new system (value-semantic) or `this`
     (stateful) without the branch. Underlying data remains until GC.
     opts: {:sync? true} — when false, returns channel/promise.")

  (checkout [this name] [this name opts]
    "Switch to branch `name` (keyword). Returns a NEW system value at the branch head
     (on every adapter). opts: {:sync? true} — when false, returns channel/promise."))

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
  "Merge support. RETURN CONTRACT mirrors `Branchable`: value-semantic systems (CRDT,
   composite) return a NEW system; stateful adapters (datahike/git) side-effect and
   return `this`. `source` is a branch KEYWORD or a snapshot/commit id (string/uuid).

   OPTS are ADVISORY and adapter-specific — not every adapter honors every key:
     :strategy :ours|:theirs|:union|fn   — recognized by graph/CRDT mergers; datahike
                                           IGNORES it (it does identity-keyed 3-way tx).
     :message  \"…\"                       — commit message where the adapter records one.
     :tx-data :tx-meta                    — datahike-specific passthrough.
     :sync? true                          — when false, returns channel/promise.
   Treat unrecognized opts as no-ops; consult the adapter for what it actually uses."

  (merge! [this source] [this source opts]
    "Merge `source` (branch keyword or snapshot/commit id) into current. Returns a new
     system (value-semantic) or `this` (stateful). Use (snapshot-id result) for the merge
     commit id. See the protocol docstring for the advisory, adapter-specific opts.")

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
  "An ISOLATED WORKSPACE over a parent system — fork → mutate → merge-down/discard.
   Distinct from Branchable: a branch is a durable named ref; an overlay is a
   transient, abandonable workspace with an observation MODE relative to the
   parent (this is the spindel-fork / OverlayBackend relationship):

     :frozen    pinned at the parent's state AT FORK TIME — the parent's later
                evolution is INVISIBLE. (A snapshot clone / branch.) Available on
                EVERY system.
     :following the workspace sees the parent's LIVE state for everything it
                hasn't overwritten, with its own writes isolated — it TRACKS the
                parent's concurrent evolution. Clean only for CONVERGENT systems
                (read = join(parent-live, own-delta), can't conflict); a VERSIONED
                system (datahike/git) can't do this cheaply and DEGRADES to :frozen
                + manual `advance!`.
     :gated     :following with an ATOMIC observation point (sequence-lock) for
                consistent reads of a moving parent. (Deferred.)

   MODE NEGOTIATION: you REQUEST a mode; each system grants it or degrades, and
   the resulting overlay reports the GRANTED mode in its `:mode`. So a composite
   overlay is honestly mixed-mode (CRDT subs :following, datahike/git subs :frozen)."

  (overlay [this opts]
    "Create an isolated overlay workspace over this system.
     opts: {:mode :frozen|:following|:gated (default :frozen), :sync? true}
     Returns an overlay whose `:mode` is the GRANTED mode. Read the effective
     value with `yggdrasil.convergent.overlay/overlay-value`, write via
     `overlay-system`, and `merge-down!`/`discard!` to finish.")

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
    "Push overlay writes to the parent. RETURNS the merged PARENT SYSTEM value (the new
     parent at the post-merge head) — re-seat your reference to it. May throw on conflict;
     pre-check with `Mergeable/conflicts`. opts: {:sync? true} — when false returns
     channel/promise (yielding the parent system).")

  (discard! [overlay] [overlay opts]
    "Abandon the overlay and all its isolated writes. RETURNS the unchanged PARENT SYSTEM
     value. opts: {:sync? true} — when false, returns channel/promise."))

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
    "Reclaim storage. `snapshot-ids` are candidate snapshots to delete (computed by
     the coordinator); storage-GC adapters that compute their own reachability
     (datahike, git) IGNORE it and reclaim everything unreachable from their branch
     heads instead. Returns an adapter-specific reclamation REPORT (not the system),
     e.g. {:system-id … :reclaimed n} — a composite returns {system-id → report}.
     opts (all optional, adapter-specific):
       :remove-before <java.util.Date> — datahike: also collapse snapshots before it
                                         (default epoch = keep all history)
       :grace-period-ms <ms>           — git: prune horizon (default git's 2 weeks)
       :dry-run?                       — report without deleting
       :sync?                          — when false, returns channel/promise"))

;; ============================================================
;; Addressable (filesystem-backed systems)
;; ============================================================

(defprotocol Addressable
  "Systems with a filesystem working directory for the current branch.
   Not all systems are addressable — e.g. IPFS, Datahike, LakeFS are not."

  (working-path [this]
    "Filesystem path (String) for the current branch's writable directory."))

;; ============================================================
;; Committable (explicit commit operations)
;; ============================================================

(defprotocol Committable
  "Create commits/snapshots. Systems that support explicit commit operations."

  (commit! [this] [this message] [this message opts]
    "Create a commit on the current branch. Returns new system value.
     opts map is system-specific (e.g. IPFS requires :root CID)."))

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

;; NOTE: the per-system runtime-mode helpers `system-sync?`/`system-async?` were
;; REMOVED — a system no longer carries an execution mode (`:opts {:sync?}` was
;; dropped from every convergent record). `:sync?` is now a PER-CALL choice on each
;; op, defaulting to `yggdrasil.convergent/default-opts`. A caller that needs the
;; mode passes it explicitly to the op (it always knew the platform).
