(ns yggdrasil.types
  "Core data types for the Yggdrasil memory model.")

;; ============================================================
;; SnapshotRef - universal reference to a point-in-time
;; ============================================================

(defrecord SnapshotRef
           [system-id       ; String - which system instance
            snapshot-id     ; String - UUID or content-hash (native format)
            parent-ids      ; Set<String> - ancestry
            hlc             ; HLC record - causal timestamp
            content-hash])  ; String? - optional, for verification/dedup

;; ============================================================
;; HLC - Hybrid Logical Clock
;; ============================================================

(defrecord HLC
           [physical        ; Long - milliseconds since epoch
            logical])       ; Int - counter for same-ms events

(defn hlc-compare
  "Lexicographic comparison of HLCs."
  [a b]
  (let [pc (compare (:physical a) (:physical b))]
    (if (zero? pc)
      (compare (:logical a) (:logical b))
      pc)))

(def ^:dynamic *now-fn*
  "Optional clock override — a 0-arg fn returning millis. `nil` ⇒ the real
   wall-clock. Bind it for deterministic spindel replay / tests (O5): a replay
   context fixes physical time rather than reading the host clock. HLC's logical
   counter still ticks under a fixed clock, so order stays monotonic."
  nil)

(defn now-ms
  "Portable wall-clock millis since epoch (JVM + cljs), via `*now-fn*` when bound."
  []
  (if *now-fn*
    (*now-fn*)
    #?(:clj (System/currentTimeMillis)
       :cljs (.getTime (js/Date.)))))

;; A single shared literal (NOT Integer/MAX_VALUE on JVM vs MAX_SAFE_INTEGER on
;; cljs): the `as-of` ceiling HLC must be byte-identical across platforms so an
;; HLC compare against a JVM-written vs cljs-written ceiling agrees, and a
;; cljs-minted `:logical` never exceeds JVM int range when serialized.
(def ^:private logical-max 2147483647)

(defn hlc-now
  "Create HLC from current time."
  []
  (->HLC (now-ms) 0))

(defn gc-cutoff
  "Resolve a GC sweep cutoff (a Date) from `opts` — the ONE convention shared by
   the whole GC machinery (protocol `gc-sweep!`, `durable/gc!`, composite, registry,
   coordinator):
     `:remove-before` (a Date)     ⇒ that exact cutoff (wins);
     `:grace-period-ms` (a window) ⇒ now − ms;
     neither                       ⇒ EPOCH ⇒ reclaim NOTHING.
   The epoch default is SAFE: a GC with no window never sweeps a node an in-flight
   lazy read might still hold. Pass a window in production (≥ your longest lazy-read
   drain; ~60 s tight, hours/a day looser) — otherwise superseded trees accumulate
   without bound."
  [opts]
  (or (:remove-before opts)
      (when-let [g (:grace-period-ms opts)]
        (#?(:clj java.util.Date. :cljs js/Date.) (- (now-ms) (long g))))
      (#?(:clj java.util.Date. :cljs js/Date.) 0)))

(defn async-gc-opts
  "Coerce caller opts for an ASYNC-ONLY gc op into async mode. GC's underlying sweep —
   `konserve.gc/sweep!` (a bare superv channel) or datahike's `gc-storage` promise — has
   NO synchronous-value form, and yggdrasil never blocks to fake one. So an EXPLICIT
   `{:sync? true}` is a misuse ⇒ THROW; otherwise force `{:sync? false}` (overriding any
   platform default). GC ops therefore return an `await`-able partial-cps CPS on BOTH
   platforms; a caller that wants to wait blocks at its OWN boundary (app/test code),
   never inside yggdrasil. `op` labels the error."
  [op opts]
  (when (true? (:sync? opts))
    (throw (ex-info (str op " has no synchronous mode — its gc sweep is async-only; "
                         "consume the async result (block at your own boundary).")
                    {:op op :opts opts})))
  (assoc opts :sync? false))

(defn hlc-tick
  "Advance HLC for local event."
  [hlc]
  (let [now (now-ms)]
    (if (> now (:physical hlc))
      (->HLC now 0)
      (->HLC (:physical hlc) (inc (:logical hlc))))))

(defn ->HLC-ceil
  "Create an HLC upper bound from a physical timestamp (millis since epoch).
   Captures all events at or before this wall-clock time, including those
   with logical counter > 0 within the same millisecond."
  [physical-ms]
  (->HLC physical-ms logical-max))

(defn hlc-receive
  "Update HLC on receiving message with remote HLC."
  [local-hlc remote-hlc]
  (let [now (now-ms)
        max-physical (max now (:physical local-hlc) (:physical remote-hlc))]
    (if (= max-physical (:physical local-hlc) (:physical remote-hlc))
      (->HLC max-physical (inc (max (:logical local-hlc) (:logical remote-hlc))))
      (if (= max-physical (:physical local-hlc))
        (->HLC max-physical (inc (:logical local-hlc)))
        (if (= max-physical (:physical remote-hlc))
          (->HLC max-physical (inc (:logical remote-hlc)))
          (->HLC max-physical 0))))))

;; ============================================================
;; Overlay - live fork state
;; ============================================================

(defrecord Overlay
           [overlay-id      ; String - unique overlay identifier
            parent          ; System - the parent system
            mode            ; :frozen | :following | :gated
            base-snapshot   ; SnapshotRef - current observation point
            local-writes    ; Atom<Delta> - isolated writes
            created-at])    ; HLC - when overlay was created

;; ============================================================
;; Capabilities
;; ============================================================

(defrecord Capabilities
           [snapshotable         ; Boolean
            branchable           ; Boolean
            graphable            ; Boolean
            mergeable            ; Boolean
            overlayable          ; Boolean
            watchable            ; Boolean
            garbage-collectable  ; Boolean
            addressable          ; Boolean
            committable])        ; Boolean

;; ============================================================
;; Conflict descriptor
;; ============================================================

(defrecord Conflict
           [path            ; Vector - location of conflict (system-specific)
            base            ; value in common ancestor
            ours            ; value in local branch
            theirs])        ; value in remote branch

;; ============================================================
;; Diff results - typed return values from Mergeable/diff
;; ============================================================

(defrecord GitDiff
           [snapshot-a      ; String - from ref
            snapshot-b      ; String - to ref
            stat            ; String - git diff --stat output
            patch           ; String - unified diff (-p) output
            files])         ; [{:status :added|:modified|:deleted, :path "..."}]

(defrecord DatahikeDiff
           [from            ; keyword/UUID - source branch/commit
            to              ; keyword/UUID - target branch/commit
            added           ; [[:db/add e a v] ...] - datoms in 'to' not in 'from'
            removed         ; [[:db/add e a v] ...] - datoms in 'from' not in 'to'
            summary])       ; {:added-datoms n, :removed-datoms n, :entities-touched n}

(defrecord DiffError
           [from            ; source ref
            to              ; target ref
            error])         ; String - error message

(defn diff-error
  "Construct a `DiffError` for a failed `diff`/`merge` between `from` and `to`.
   Supported constructor — use this instead of `->DiffError` so the record's field
   shape can evolve without breaking callers (e.g. spindel's diff bridge)."
  [from to error]
  (->DiffError from to error))

;; ============================================================
;; Registry entry - cross-system snapshot tracking
;; ============================================================

(defrecord RegistryEntry
           [snapshot-id     ; String - native ID from the system
            system-id       ; String - p/system-id
            branch-name     ; String - branch name
            hlc             ; HLC - causal timestamp
            content-hash    ; String? - optional dedup hash
            parent-ids      ; #{String} - parent snapshot IDs
            metadata])      ; Map - message, author, etc.

;; ============================================================
;; JSON serialization
;; ============================================================

(defn snapshot-ref->json
  "Convert SnapshotRef to JSON-compatible map."
  [ref]
  {"system-id" (:system-id ref)
   "snapshot-id" (:snapshot-id ref)
   "parent-ids" (vec (:parent-ids ref))
   "hlc" (when-let [h (:hlc ref)]
           {"physical" (:physical h)
            "logical" (:logical h)})
   "content-hash" (:content-hash ref)})

(defn json->snapshot-ref
  "Parse SnapshotRef from JSON-compatible map."
  [m]
  (->SnapshotRef
   (get m "system-id")
   (get m "snapshot-id")
   (set (get m "parent-ids"))
   (when-let [h (get m "hlc")]
     (->HLC (get h "physical") (get h "logical")))
   (get m "content-hash")))
