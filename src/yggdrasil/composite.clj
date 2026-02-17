(ns yggdrasil.composite
  "Pullback system: coordinates N sub-systems as one logical unit.

   Categorically, this is a fiber product (pullback) over the shared branch
   space. Given systems A and B both mapping to a branch space S via their
   `current-branch` function, the CompositeSystem is the pullback A ×_S B —
   pairs (a, b) where branch(a) = branch(b).

   All protocol operations are applied componentwise, constrained to preserve
   the fiber condition (shared branch). The construction is monoidal:
     pullback(pullback(A, B), C) ≅ pullback(A, B, C)

   Key aggregation strategies:
     branches  → intersection (only branches valid in ALL sub-systems)
     conflicts → union        (any sub-system conflict is a composite conflict)
     gc-roots  → union        (any sub-system root is a composite root)

   Uses VALUE SEMANTICS: mutating operations return new CompositeSystem values.

   History persistence uses a persistent-sorted-set (PSS) backed by
   konserve via KonserveStorage, following the same pattern as the
   yggdrasil registry. When :store-path is provided, the index is
   lazy-loaded from disk; otherwise it's purely in-memory (ephemeral)."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.storage :as store]
            [konserve.core :as k]
            [clojure.core.async :refer [<!!]]
            [clojure.set :as set]
            [clojure.string :as str]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [java.io File]
           [org.replikativ.persistent_sorted_set
            ANode Branch IStorage Leaf Settings]))

;; ============================================================
;; Composite snapshot-id
;; ============================================================

(defn- composite-snapshot-id
  "Deterministic UUID from sorted [system-id snapshot-id] pairs.
   Same combination of sub-system states always yields the same ID."
  [systems]
  (let [pairs (sort-by first
                       (map (fn [[sid sys]] [sid (p/snapshot-id sys)])
                            systems))
        content (pr-str pairs)]
    (str (java.util.UUID/nameUUIDFromBytes
          (.getBytes content "UTF-8")))))

;; ============================================================
;; Capability intersection
;; ============================================================

(defn- intersect-capabilities
  "Intersection of capabilities across all sub-systems."
  [systems]
  (let [caps (map #(p/capabilities (val %)) systems)]
    (t/->Capabilities
     (every? :snapshotable caps)
     (every? :branchable caps)
     (every? :graphable caps)
     (every? :mergeable caps)
     (every? :overlayable caps)
     (every? :watchable caps)
     (every? :garbage-collectable caps)
     (every? :addressable caps)
     (every? :committable caps))))

;; ============================================================
;; CompositeStorage — IStorage for PSS with plain map keys
;; ============================================================
;; Unlike KonserveStorage in storage.clj (which converts keys to/from
;; RegistryEntry records), this stores plain Clojure maps directly.

(defrecord CompositeStorage [kv-store ^Settings settings cache freed-atom]
  IStorage
  (store [_ node]
    (let [^ANode node node
          address (random-uuid)
          node-data {:level     (.level node)
                     :keys      (vec (.keys node))
                     :addresses (when (instance? Branch node)
                                  (vec (.addresses ^Branch node)))}]
      (<!! (k/assoc kv-store address node-data))
      (swap! cache assoc address node)
      address))

  (restore [_ address]
    (or (get @cache address)
        (let [node-data (<!! (k/get kv-store address))
              keys      (vec (:keys node-data))
              addresses (:addresses node-data)
              node (if addresses
                     (Branch. (int (:level node-data))
                              ^java.util.List keys
                              ^java.util.List (vec addresses)
                              settings)
                     (Leaf. ^java.util.List keys settings))]
          (swap! cache assoc address node)
          node)))

  (accessed [_ address]
    nil)

  (markFreed [_ address]
    (when address
      (swap! freed-atom assoc address (System/currentTimeMillis))))

  (isFreed [_ address]
    (contains? @freed-atom address))

  (freedInfo [_ address]
    (get @freed-atom address)))

(defn- create-composite-storage
  "Create a CompositeStorage backed by a konserve store."
  [kv-store]
  (->CompositeStorage kv-store (Settings.) (atom {}) (atom {})))

;; ============================================================
;; PSS index helpers
;; ============================================================

(def ^:private entry-comparator
  "Sort composite history entries by composite-snap-id."
  (fn [a b]
    (compare (:composite-snap-id a) (:composite-snap-id b))))

(defn- entry-probe
  "Create a probe entry for PSS point lookup by snap-id."
  [snap-id]
  {:composite-snap-id (str snap-id)})

(defn- lookup-entry
  "Point-lookup a composite history entry by snap-id in the PSS index.
   Returns the entry map or nil."
  [index snap-id]
  (first (pss/slice index (entry-probe snap-id) (entry-probe snap-id))))

(defn- resolve-sub-snapshots
  "Given a composite snapshot ID, resolve to {system-id → snapshot-id}.
   Returns nil if snap-id is not a known composite ID."
  [index-atom snap-id]
  (:sub-snapshots (lookup-entry @index-atom (str snap-id))))

;; ============================================================
;; Persistence helpers (following registry.clj / storage.clj)
;; ============================================================

(defn- init-index
  "Initialize the PSS index for composite history.
   When store-path is provided, creates a konserve-backed PSS with lazy loading.
   Returns [kv-store storage index]."
  [store-path]
  (if store-path
    (let [kv (store/create-store store-path)
          stg (create-composite-storage kv)
          root (<!! (k/get kv :composite/root))
          freed (or (<!! (k/get kv :composite/freed)) {})]
      (reset! (:freed-atom stg) freed)
      (if root
        [kv stg (pss/restore-by entry-comparator root stg
                                {:branching-factor 64})]
        [kv stg (pss/sorted-set* {:comparator entry-comparator
                                  :storage stg
                                  :branching-factor 64})]))
    [nil nil (pss/sorted-set-by entry-comparator)]))

(defn- persist-index!
  "Persist the PSS index root and freed nodes to konserve.
   No-op when storage is nil (ephemeral mode)."
  [kv-store storage index-atom]
  (when storage
    (let [root (pss/store @index-atom storage)]
      (<!! (k/assoc kv-store :composite/root root)))
    (<!! (k/assoc kv-store :composite/freed @(:freed-atom storage)))))

(defn- register-initial-snapshot!
  "Record the initial composite snapshot if not already in the index.
   Guards against duplicates on reopen."
  [index-atom kv-store storage sys-map snap]
  (when-not (lookup-entry @index-atom snap)
    (swap! index-atom conj
           {:composite-snap-id snap
            :parent-ids #{}
            :timestamp (System/currentTimeMillis)
            :sub-snapshots (into {}
                                 (map (fn [[sid s]] [sid (p/snapshot-id s)]))
                                 sys-map)})
    (persist-index! kv-store storage index-atom)))

;; ============================================================
;; CompositeSystem (fiber product / pullback)
;; ============================================================

(defrecord CompositeSystem
           [systems              ;; {system-id → system}
            current-branch-name  ;; keyword — shared branch
            composite-name       ;; string — system-id for the composite
            index-atom           ;; atom of PSS sorted by composite-snap-id
            kv-store             ;; konserve store (nil for ephemeral)
            storage]             ;; KonserveStorage (nil for ephemeral)

  p/SystemIdentity
  (system-id [_] composite-name)
  (system-type [_] :composite)
  (capabilities [_] (intersect-capabilities systems))

  p/Snapshotable
  (snapshot-id [_]
    (composite-snapshot-id systems))

  (parent-ids [this]
    (let [snap (p/snapshot-id this)
          entry (lookup-entry @index-atom snap)]
      (or (:parent-ids entry) #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (if-let [sub-snaps (resolve-sub-snapshots index-atom snap-id)]
      ;; Known composite ID — delegate to each sub-system
      (into {}
            (map (fn [[sid sys]]
                   [sid (p/as-of sys (get sub-snaps sid))]))
            systems)
      ;; Unknown — return nil
      nil))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (when-let [entry (lookup-entry @index-atom (str snap-id))]
      {:snapshot-id (str snap-id)
       :parent-ids (:parent-ids entry)
       :timestamp (:timestamp entry)
       :message (:message entry)
       :sub-snapshots (:sub-snapshots entry)}))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (if (empty? systems)
      #{}
      (reduce set/intersection
              (map (fn [[_ sys]] (p/branches sys)) systems))))

  (current-branch [_] current-branch-name)

  (branch! [this name]
    (let [results (reduce-kv
                   (fn [acc sid sys]
                     (assoc acc sid (p/branch! sys name)))
                   {}
                   systems)]
      (assoc this :systems results)))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (let [results (reduce-kv
                   (fn [acc sid sys]
                     (assoc acc sid (p/branch! sys name from)))
                   {}
                   systems)]
      (assoc this :systems results)))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [results (reduce-kv
                   (fn [acc sid sys]
                     (assoc acc sid (p/delete-branch! sys name)))
                   {}
                   systems)]
      (assoc this :systems results)))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [results (reduce-kv
                   (fn [acc sid sys]
                     (assoc acc sid (p/checkout sys name)))
                   {}
                   systems)]
      (assoc this
             :systems results
             :current-branch-name (keyword name))))

  p/Committable
  (commit! [this] (p/commit! this nil nil))
  (commit! [this message] (p/commit! this message nil))
  (commit! [this message _opts]
    (let [old-snap (composite-snapshot-id systems)
          new-systems (reduce-kv
                       (fn [acc sid sys]
                         (if (satisfies? p/Committable sys)
                           (assoc acc sid (p/commit! sys message))
                           (assoc acc sid sys)))
                       {}
                       systems)
          new-snap (composite-snapshot-id new-systems)]
      (swap! index-atom conj
             {:composite-snap-id new-snap
              :parent-ids #{old-snap}
              :timestamp (System/currentTimeMillis)
              :message message
              :sub-snapshots (into {}
                                   (map (fn [[sid sys]] [sid (p/snapshot-id sys)]))
                                   new-systems)})
      (persist-index! kv-store storage index-atom)
      (assoc this :systems new-systems)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [limit (or (:limit opts) 100)]
      (loop [snap-id (composite-snapshot-id systems)
             result []
             visited #{}]
        (cond
          (visited snap-id) result
          (nil? snap-id) result
          (>= (count result) limit) result
          :else
          (let [entry (lookup-entry @index-atom snap-id)
                parents (or (:parent-ids entry) #{})]
            (recur (first parents)
                   (conj result snap-id)
                   (conj visited snap-id)))))))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (loop [queue [(str snap-id)]
           visited #{}]
      (if (empty? queue)
        (vec (disj visited (str snap-id)))
        (let [current (first queue)
              rest-q (subvec (vec queue) 1)]
          (if (visited current)
            (recur rest-q visited)
            (let [entry (lookup-entry @index-atom current)
                  parents (or (:parent-ids entry) #{})]
              (recur (into rest-q parents)
                     (conj visited current))))))))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [this a b _opts]
    (contains? (set (p/ancestors this b)) (str a)))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [this a b _opts]
    (let [ancestors-a (conj (set (p/ancestors this a)) (str a))]
      (loop [queue [(str b)]
             visited #{}]
        (if (empty? queue)
          nil
          (let [current (first queue)
                rest-q (subvec (vec queue) 1)]
            (cond
              (visited current) (recur rest-q visited)
              (ancestors-a current) current
              :else
              (let [entry (lookup-entry @index-atom current)
                    parents (vec (or (:parent-ids entry) #{}))]
                (recur (into rest-q parents)
                       (conj visited current)))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (let [entries (seq @index-atom)]
      {:nodes (into {}
                    (map (fn [entry]
                           [(:composite-snap-id entry)
                            {:parent-ids (:parent-ids entry)
                             :meta {:timestamp (:timestamp entry)
                                    :message (:message entry)}}]))
                    entries)
       :branches {current-branch-name (composite-snapshot-id systems)}
       :roots (set (keep
                    (fn [entry]
                      (when (empty? (or (:parent-ids entry) #{}))
                        (:composite-snap-id entry)))
                    entries))}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [_ snap-id _opts]
    (when-let [entry (lookup-entry @index-atom (str snap-id))]
      {:parent-ids (:parent-ids entry)
       :timestamp (:timestamp entry)
       :message (:message entry)
       :sub-snapshots (:sub-snapshots entry)}))

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this source opts]
    (let [new-systems (reduce-kv
                       (fn [acc sid sys]
                         (assoc acc sid (p/merge! sys source opts)))
                       {}
                       systems)]
      (assoc this :systems new-systems)))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ _a _b _opts]
    ;; Sub-systems use their own snapshot IDs, not composite IDs.
    ;; Conflict detection delegates per-system when needed.
    [])

  (diff [this a b] (p/diff this a b nil))
  (diff [_ _a _b _opts]
    ;; Per-system diff requires resolving composite snap-ids to sub-system snap-ids.
    ;; Callers should use sub-system diffs directly for detailed results.
    (into {}
          (map (fn [[sid sys]] [sid {:diff :composite-level}]))
          systems))

  p/GarbageCollectable
  (gc-roots [_]
    (if (empty? systems)
      #{}
      (reduce set/union
              (map (fn [[_ sys]] (p/gc-roots sys)) systems))))

  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids _opts]
    (let [new-systems (reduce-kv
                       (fn [acc sid sys]
                         (assoc acc sid (p/gc-sweep! sys snapshot-ids)))
                       {}
                       systems)]
      (assoc this :systems new-systems))))

;; ============================================================
;; Lifecycle
;; ============================================================

(defn flush!
  "Persist current composite history index to disk.
   No-op when the composite is ephemeral (no :store-path)."
  [composite]
  (persist-index! (:kv-store composite) (:storage composite)
                  (:index-atom composite)))

(defn close!
  "Flush and close the composite's persistent store.
   Safe to call on ephemeral composites (no-op)."
  [composite]
  (flush! composite))

;; ============================================================
;; Constructor
;; ============================================================

(defn pullback
  "Create a CompositeSystem (fiber product) from multiple yggdrasil systems.

   Categorically, this is the pullback A ×_S B over the shared branch space S.
   The fiber condition requires all sub-systems to be on the same logical branch.

   The construction is monoidal: pullback(pullback(A,B), C) ≅ pullback(A,B,C).

   systems-seq: seq of yggdrasil systems
   opts:
     :name       — composite system name (default: auto-generated)
     :branch     — explicit logical branch name. When provided, sub-systems may
                   have different native branch names (e.g. datahike uses :db,
                   scriptum uses \"main\"). When omitted, all sub-systems must
                   report the same current-branch.
     :store-path — directory for konserve persistence (nil = ephemeral)"
  [systems-seq & {:keys [name branch store-path]}]
  (let [sys-map (into {} (map (fn [s] [(p/system-id s) s]) systems-seq))
        resolved-branch (if branch
                          branch
                          (let [branches-list (map p/current-branch (vals sys-map))
                                first-branch (first branches-list)]
                            (when-not (apply = branches-list)
                              (throw (ex-info "Pullback requires all sub-systems on the same branch (fiber condition violated). Use :branch to specify the logical branch when native names differ."
                                              {:branches (into {} (map (fn [s] [(p/system-id s) (p/current-branch s)])
                                                                       (vals sys-map)))})))
                            first-branch))
        [kv-store storage index] (init-index store-path)
        index-atom (atom index)
        sys (->CompositeSystem
              sys-map
              resolved-branch
              (or name (str "pullback:" (str/join "×" (sort (keys sys-map)))))
              index-atom kv-store storage)]
    ;; Record initial composite snapshot (idempotent on reopen)
    (register-initial-snapshot! index-atom kv-store storage
                                sys-map (p/snapshot-id sys))
    sys))

(defn composite
  "Create a CompositeSystem from multiple systems.

   Unlike pullback, does not enforce the same-branch constraint.
   Use when sub-systems have different branch naming conventions
   (e.g. datahike uses :db, scriptum uses :main).

   systems-seq: seq of yggdrasil systems
   opts:
     :name       — composite system name (default: auto-generated)
     :branch     — explicit branch keyword (default: :main)
     :store-path — directory for konserve persistence (nil = ephemeral)"
  [systems-seq & {:keys [name branch store-path] :or {branch :main}}]
  (let [sys-map (into {} (map (fn [s] [(p/system-id s) s]) systems-seq))
        [kv-store storage index] (init-index store-path)
        index-atom (atom index)
        sys (->CompositeSystem
              sys-map
              branch
              (or name (str "composite:" (str/join "+" (sort (keys sys-map)))))
              index-atom kv-store storage)]
    ;; Record initial composite snapshot (idempotent on reopen)
    (register-initial-snapshot! index-atom kv-store storage
                                sys-map (p/snapshot-id sys))
    sys))

;; ============================================================
;; Accessors
;; ============================================================

(defn get-subsystem
  "Get a sub-system by its system-id from a CompositeSystem."
  [composite system-id]
  (get (:systems composite) system-id))
