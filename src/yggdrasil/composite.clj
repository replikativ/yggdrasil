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

   Uses VALUE SEMANTICS: mutating operations return new CompositeSystem values."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [clojure.set :as set]
            [clojure.string :as str]))

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
;; Sub-snapshot resolution
;; ============================================================

(defn- resolve-sub-snapshots
  "Given a composite snapshot ID, resolve to {system-id → snapshot-id}.
   Returns nil if snap-id is not a known composite ID."
  [history-atom snap-id]
  (get-in @history-atom [(str snap-id) :sub-snapshots]))

;; ============================================================
;; CompositeSystem (fiber product / pullback)
;; ============================================================

(defrecord CompositeSystem
           [systems              ;; {system-id → system}
            current-branch-name  ;; keyword — shared branch
            composite-name       ;; string — system-id for the composite
            history-atom]        ;; atom of {composite-snap-id → {:parent-ids #{} :timestamp long
                        ;;                               :sub-snapshots {sid → snap-id}}}

  p/SystemIdentity
  (system-id [_] composite-name)
  (system-type [_] :composite)
  (capabilities [_] (intersect-capabilities systems))

  p/Snapshotable
  (snapshot-id [_]
    (composite-snapshot-id systems))

  (parent-ids [this]
    (let [snap (p/snapshot-id this)]
      (get-in @history-atom [snap :parent-ids] #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (if-let [sub-snaps (resolve-sub-snapshots history-atom snap-id)]
      ;; Known composite ID — delegate to each sub-system
      (into {}
            (map (fn [[sid sys]]
                   [sid (p/as-of sys (get sub-snaps sid))]))
            systems)
      ;; Unknown — return nil
      nil))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (when-let [entry (get @history-atom (str snap-id))]
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
      (swap! history-atom assoc new-snap
             {:parent-ids #{old-snap}
              :timestamp (System/currentTimeMillis)
              :message message
              :sub-snapshots (into {}
                                   (map (fn [[sid sys]] [sid (p/snapshot-id sys)]))
                                   new-systems)})
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
          (let [entry (get @history-atom snap-id)
                parents (:parent-ids entry #{})]
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
            (let [parents (get-in @history-atom [current :parent-ids] #{})]
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
              (let [parents (vec (get-in @history-atom [current :parent-ids] #{}))]
                (recur (into rest-q parents)
                       (conj visited current)))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (let [hist @history-atom]
      {:nodes (into {}
                    (map (fn [[id entry]]
                           [id {:parent-ids (:parent-ids entry)
                                :meta {:timestamp (:timestamp entry)
                                       :message (:message entry)}}]))
                    hist)
       :branches {current-branch-name (composite-snapshot-id systems)}
       :roots (set (filter
                    (fn [id]
                      (empty? (get-in hist [id :parent-ids] #{})))
                    (keys hist)))}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [_ snap-id _opts]
    (when-let [entry (get @history-atom (str snap-id))]
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
;; Constructor
;; ============================================================

(defn pullback
  "Create a CompositeSystem (fiber product) from multiple yggdrasil systems.

   Categorically, this is the pullback A ×_S B over the shared branch space S.
   The fiber condition requires all sub-systems to be on the same logical branch.

   The construction is monoidal: pullback(pullback(A,B), C) ≅ pullback(A,B,C).

   systems-seq: seq of yggdrasil systems
   opts:
     :name   — composite system name (default: auto-generated)
     :branch — explicit logical branch name. When provided, sub-systems may
               have different native branch names (e.g. datahike uses :db,
               scriptum uses \"main\"). When omitted, all sub-systems must
               report the same current-branch."
  [systems-seq & {:keys [name branch]}]
  (let [sys-map (into {} (map (fn [s] [(p/system-id s) s]) systems-seq))
        resolved-branch (if branch
                          branch
                          (let [branches-list (map p/current-branch (vals sys-map))
                                first-branch (first branches-list)]
                            (when-not (apply = branches-list)
                              (throw (ex-info "Pullback requires all sub-systems on the same branch (fiber condition violated). Use :branch to specify the logical branch when native names differ."
                                              {:branches (into {} (map (fn [s] [(p/system-id s) (p/current-branch s)])
                                                                       (vals sys-map)))})))
                            first-branch))]
    (let [sys (->CompositeSystem
               sys-map
               resolved-branch
               (or name (str "pullback:" (str/join "×" (sort (keys sys-map)))))
               (atom {}))]
      ;; Record initial composite snapshot
      (let [snap (p/snapshot-id sys)]
        (swap! (:history-atom sys) assoc snap
               {:parent-ids #{}
                :timestamp (System/currentTimeMillis)
                :sub-snapshots (into {}
                                     (map (fn [[sid s]] [sid (p/snapshot-id s)]))
                                     sys-map)}))
      sys)))

(defn composite
  "Create a CompositeSystem from multiple systems.

   Unlike pullback, does not enforce the same-branch constraint.
   Use when sub-systems have different branch naming conventions
   (e.g. datahike uses :db, scriptum uses :main).

   systems-seq: seq of yggdrasil systems
   opts:
     :name   — composite system name (default: auto-generated)
     :branch — explicit branch keyword (default: :main)"
  [systems-seq & {:keys [name branch] :or {branch :main}}]
  (let [sys-map (into {} (map (fn [s] [(p/system-id s) s]) systems-seq))
        sys (->CompositeSystem
             sys-map
             branch
             (or name (str "composite:" (str/join "+" (sort (keys sys-map)))))
             (atom {}))]
    (let [snap (p/snapshot-id sys)]
      (swap! (:history-atom sys) assoc snap
             {:parent-ids #{}
              :timestamp (System/currentTimeMillis)
              :sub-snapshots (into {}
                                   (map (fn [[sid s]] [sid (p/snapshot-id s)]))
                                   sys-map)}))
    sys))

;; ============================================================
;; Accessors
;; ============================================================

(defn get-subsystem
  "Get a sub-system by its system-id from a CompositeSystem."
  [composite system-id]
  (get (:systems composite) system-id))
