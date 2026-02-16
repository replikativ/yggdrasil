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

(defn hlc-now
  "Create HLC from current time."
  []
  (->HLC (System/currentTimeMillis) 0))

(defn hlc-tick
  "Advance HLC for local event."
  [hlc]
  (let [now (System/currentTimeMillis)]
    (if (> now (:physical hlc))
      (->HLC now 0)
      (->HLC (:physical hlc) (inc (:logical hlc))))))

(defn hlc-receive
  "Update HLC on receiving message with remote HLC."
  [local-hlc remote-hlc]
  (let [now (System/currentTimeMillis)
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
            garbage-collectable]) ; Boolean

;; ============================================================
;; Conflict descriptor
;; ============================================================

(defrecord Conflict
           [path            ; Vector - location of conflict (system-specific)
            base            ; value in common ancestor
            ours            ; value in local branch
            theirs])        ; value in remote branch

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
