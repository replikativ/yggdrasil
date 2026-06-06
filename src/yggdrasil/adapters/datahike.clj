(ns yggdrasil.adapters.datahike
  "Datahike adapter for Yggdrasil protocols.

  Wraps a Datahike connection (atom of db state) and exposes
  Snapshotable, Branchable, Graphable, Mergeable, and GarbageCollectable.

  Also extends the yggdrasil.hooks multimethod to use datahike's native
  d/listen for immediate commit notification (no polling needed).

  Requires datahike on the classpath. Only load this namespace when
  datahike is available as a dependency."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.hooks :as hooks]
            [konserve.core :as k]
            [datahike.api :as d]
            [datahike.versioning :as dv]
            [datahike.writing :as dw])
  (:import [yggdrasil.types DatahikeDiff DiffError]))

;; ============================================================
;; Internal helpers
;; ============================================================

(defn- store-of [conn]
  (:store @conn))

(defn- db-of [conn]
  @conn)

(defn- branch-of [conn]
  (get-in @conn [:config :branch]))

(defn- commit-id-of [db]
  (get-in db [:meta :datahike/commit-id]))

(defn- parent-ids-of [db]
  (get-in db [:meta :datahike/parents]))

;; ============================================================
;; Branch diff computation
;; ============================================================

(defn- compute-branch-diff
  "Compute datoms in source-db that are not in target-db.
   Returns vector of [:db/add e a v] transaction data."
  [source-db target-db]
  (let [diff (d/q '[:find ?e ?a ?v
                    :in $ $2
                    :where
                    [$ ?e ?a ?v]
                    [(not= :db/txInstant ?a)]
                    (not [$2 ?e ?a ?v])]
                  source-db target-db)]
    (mapv (fn [[e a v]] [:db/add e a v]) diff)))

;; --- identity-keyed merge (sibling-safe) ----------------------------------
;; Raw [:db/add e a v] tx-data collides across SIBLING branches: each branch
;; allocates entity-ids sequentially from the shared fork point, so two forks
;; mint the SAME ?e for different entities → the second merge clobbers the
;; first. The fix: address merged entities by their `:db.unique/identity`
;; lookup-ref, so datahike unions them by SEMANTIC id. (Additions only here —
;; append-only data like a conversation. Entities CHANGED on both sides are a
;; 3-way conflict, surfaced via `conflicts` + reconciled by an agent.)

(defn- schema-attrs
  "{:unique #{idents} :ref #{idents} :component #{idents}} for db's schema."
  [db]
  (let [rows (d/q '[:find ?id ?uniq ?vt ?comp
                    :where
                    [?a :db/ident ?id]
                    [(get-else $ ?a :db/unique :none) ?uniq]
                    [(get-else $ ?a :db/valueType :none) ?vt]
                    [(get-else $ ?a :db/isComponent false) ?comp]]
                  db)]
    {:unique    (set (keep (fn [[id u]]    (when (= u :db.unique/identity) id)) rows))
     :ref       (set (keep (fn [[id _ vt]] (when (= vt :db.type/ref) id)) rows))
     :component (set (keep (fn [[id _ _ c]] (when c id)) rows))}))

(defn- entity-ident
  "Lookup-ref [uattr uval] identifying entity `e`, or nil if it has none."
  [db unique e]
  (let [ent (d/entity db e)]
    (some (fn [ua] (when-some [uv (get ent ua)] [ua uv])) unique)))

(defn- prepare-entity
  "Pull entity `e` and return a tx-map keyed by its unique identity, with
   non-component refs resolved to lookup-refs and components kept nested.
   nil if `e` carries no unique identity."
  [db {:keys [unique ref component] :as sch} e]
  (letfn [(prep [m]
            (into {}
                  (keep (fn [[k v]]
                          (cond
                            (= k :db/id) nil
                            (contains? ref k)
                            (let [one (fn [x]
                                        (cond
                                          (contains? component k) (prep x) ; nested component
                                          (map? x) (entity-ident db unique (:db/id x))
                                          :else x))]
                              (if (vector? v)
                                (when-let [vs (seq (keep one v))] [k (vec vs)])
                                (when-let [rv (one v)] [k rv])))
                            :else [k v])))
                  m))]
    (let [m (prep (d/pull db '[*] e))]
      (when (some (fn [[k]] (contains? unique k)) m) m))))

(defn- compute-merge-tx
  "Merge tx-data for datoms in source not in target:
   - entities carrying a `:db.unique/identity` attr are pulled and re-keyed by
     their lookup-ref, so concurrent sibling branches UNION by semantic id rather
     than colliding on entity-id (their components ride along nested);
   - anonymous entities (no identity, and not a component of an identity entity)
     are carried via a fresh tempid so they're added without entity-id collision
     (no semantic key to dedup on — degenerate but lossless)."
  [source-db target-db]
  (let [{:keys [unique component] :as sch} (schema-attrs source-db)
        id-maps        (for [ua unique
                             [e uv] (d/q '[:find ?e ?uv :in $ ?ua :where [?e ?ua ?uv]] source-db ua)
                             :when  (empty? (d/q '[:find ?t :in $ ?ua ?uv :where [?t ?ua ?uv]] target-db ua uv))]
                         (prepare-entity source-db sch e))
        ;; component entities are reached via their identity parent's pull
        component-eids (set (when (seq component)
                              (d/q '[:find [?c ...] :in $ [?ca ...] :where [_ ?ca ?c]] source-db (vec component))))
        ref?           (:ref sch)
        resolve-v      (fn [a v] (if (and (ref? a) (integer? v))
                                   (or (entity-ident source-db unique v) v)
                                   v))
        anon-datoms    (->> (d/q '[:find ?e ?a ?v :in $ $2 :where
                                   [$ ?e ?a ?v] [(not= :db/txInstant ?a)] (not [$2 ?e ?a ?v])]
                                 source-db target-db)
                            (remove (fn [[e]] (or (entity-ident source-db unique e)
                                                  (component-eids e)))))
        anon-tx        (->> (group-by first anon-datoms)
                            (mapcat (fn [[e datoms]]
                                      (let [tid (str "ygg-tmp-" e)]
                                        (map (fn [[_ a v]] [:db/add tid a (resolve-v a v)]) datoms)))))]
    (vec (concat (remove nil? id-maps) anon-tx))))

;; ============================================================
;; History traversal (synchronous, bounded)
;; ============================================================

(defn- walk-history
  "Walk commit graph from starting refs, collecting snapshot-ids.
   Returns vector of commit-id STRINGS in traversal order.

   The queue/visited carry the raw refs (branch keyword or commit UUID) so
   konserve `k/get` can load each node, but the RESULT is stringified — the
   protocol's snapshot-ids are strings (`snapshot-id` returns `(str …)`), and
   every consumer (`ancestors`, `ancestor?`, `common-ancestor`, `commit-graph`)
   compares with `(str …)`. Returning UUID objects here silently broke all of
   them (a UUID never equals its own string in a set lookup → common-ancestor
   always returned nil)."
  [store start-refs {:keys [limit] :or {limit 100}}]
  (loop [queue (vec start-refs)
         visited #{}
         result []]
    (if (or (empty? queue)
            (and limit (>= (count result) limit)))
      result
      (let [[current & rest] queue]
        (if (visited current)
          (recur (vec rest) visited result)
          (if-let [raw-db (k/get store current nil {:sync? true})]
            (let [db (dw/stored->db raw-db store)
                  parents (parent-ids-of db)]
              (recur (into (vec rest) parents)
                     (conj visited current)
                     (conj result (str (commit-id-of db)))))
            (recur (vec rest) (conj visited current) result)))))))

;; ============================================================
;; DatahikeSystem record
;; ============================================================

(defrecord DatahikeSystem [conn system-name]
  p/SystemIdentity
  (system-id [_]
    (or system-name
        (str "datahike:" (get-in @conn [:config :store :id]))))
  (system-type [_] :datahike)
  (capabilities [_]
    (t/->Capabilities true true true true false false true false false))

  p/Snapshotable
  (snapshot-id [_]
    (str (commit-id-of (db-of conn))))

  (parent-ids [_]
    (let [parents (parent-ids-of (db-of conn))]
      (set (map str parents))))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (let [store (store-of conn)
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (dv/commit-as-db store uuid)))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [store (store-of conn)
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (when-let [db (dv/commit-as-db store uuid)]
        {:snapshot-id (str (commit-id-of db))
         :parent-ids (set (map str (parent-ids-of db)))
         :timestamp (get-in db [:meta :datahike/updated-at])
         :branch (get-in db [:config :branch])})))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (let [store (store-of conn)]
      (k/get store :branches nil {:sync? true})))

  (current-branch [_]
    (branch-of conn))

  (branch! [this name]
    (dv/branch! conn (branch-of conn) name)
    this)

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (dv/branch! conn from name)
    this)

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [_ name _opts]
    (dv/delete-branch! conn name))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [current-cfg (:config @conn)
          branch-cfg (assoc current-cfg :branch name)
          branch-conn (d/connect branch-cfg)]
      (->DatahikeSystem branch-conn system-name)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [store (store-of conn)
          branch (branch-of conn)]
      (walk-history store [branch] opts)))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [store (store-of conn)
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (walk-history store [uuid] {:limit nil})))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (let [store (store-of conn)
          uuid-a (if (uuid? a) a (parse-uuid (str a)))
          uuid-b (if (uuid? b) b (parse-uuid (str b)))
          ancestors-of-b (set (walk-history store [uuid-b] {:limit nil}))]
      (contains? ancestors-of-b (str uuid-a))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (let [store (store-of conn)
          uuid-a (if (uuid? a) a (parse-uuid (str a)))
          uuid-b (if (uuid? b) b (parse-uuid (str b)))
          ancestors-a (set (walk-history store [uuid-a] {:limit nil}))]
      (loop [queue [uuid-b]
             visited #{}]
        (when (seq queue)
          (let [[current & rest] queue]
            (if (visited current)
              (recur (vec rest) visited)
              (if (ancestors-a (str current))
                (str current)
                (if-let [db (dv/commit-as-db (store-of conn) current)]
                  (recur (into (vec rest) (parent-ids-of db))
                         (conj visited current))
                  (recur (vec rest) (conj visited current))))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [this _opts]
    (let [store (store-of conn)
          branches (p/branches this)
          all-ids (walk-history store (vec branches) {:limit nil})]
      {:nodes (into {}
                    (for [id all-ids
                          :let [uuid (parse-uuid id)
                                db (when uuid (dv/commit-as-db store uuid))]
                          :when db]
                      [id {:parent-ids (set (map str (parent-ids-of db)))
                           :meta (p/snapshot-meta this id)}]))
       :branches (into {}
                       (for [b branches]
                         [b (when-let [db (k/get store b nil {:sync? true})]
                              (str (commit-id-of (dw/stored->db db store))))]))
       :roots (set (filter
                    (fn [id]
                      (let [uuid (parse-uuid id)
                            db (when uuid (dv/commit-as-db store uuid))]
                        (or (nil? db) (empty? (parent-ids-of db)))))
                    all-ids))}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (p/snapshot-meta this snap-id))

  p/GarbageCollectable
  (gc-roots [_]
    (let [store (store-of conn)
          branches (k/get store :branches nil {:sync? true})]
      (->> branches
           (map (fn [branch]
                  (when-let [raw-db (k/get store branch nil {:sync? true})]
                    (str (commit-id-of (dw/stored->db raw-db store))))))
           (remove nil?)
           set)))

  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids _opts]
    ;; Datahike manages its own GC internally.
    ;; Actual storage reclamation happens through Datahike's gc-storage!.
    this)

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this source opts]
    (let [store (store-of conn)
          source-branch (if (keyword? source) source nil)
          parents (if (keyword? source)
                    #{source}
                    #{(if (uuid? source) source (parse-uuid (str source)))})
          tx-data (or (:tx-data opts)
                      (when source-branch
                        (let [source-db (dv/branch-as-db store source-branch)
                              target-db (db-of conn)]
                          ;; identity-keyed (sibling-safe), not raw [:db/add e a v]
                          (compute-merge-tx source-db target-db)))
                      [])]
      (dv/merge! conn parents tx-data (:tx-meta opts))
      this))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b _opts]
    [])

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    (let [store (store-of conn)
          resolve-db (fn [x]
                       (cond
                         (keyword? x) (dv/branch-as-db store x)
                         (uuid? x)    (dv/commit-as-db store x)
                         :else        (when-let [u (parse-uuid (str x))]
                                        (dv/commit-as-db store u))))
          db-a (resolve-db a)
          db-b (resolve-db b)]
      (if (and db-a db-b)
        (let [added   (compute-branch-diff db-b db-a)
              removed (compute-branch-diff db-a db-b)]
          (t/->DatahikeDiff
           a b added removed
           {:added-datoms (count added)
            :removed-datoms (count removed)
            :entities-touched (count (into (set (map second added))
                                           (map second removed)))}))
        (t/->DiffError a b "Could not resolve branch/snapshot")))))

;; ============================================================
;; Constructor
;; ============================================================

(defn create
  "Create a Datahike adapter from an existing connection.

   (create conn)
   (create conn {:system-name \"my-datahike-db\"})"
  ([conn] (create conn {}))
  ([conn opts]
   (->DatahikeSystem conn (:system-name opts))))

;; ============================================================
;; Native commit hook via d/listen
;; ============================================================

(defmethod hooks/install-commit-hook! :datahike
  [_workspace system on-commit-fn]
  (let [conn (:conn system)
        listener-key (keyword (str "yggdrasil-" (p/system-id system)))]
    (d/listen conn listener-key
              (fn [tx-report]
                (when-let [db (:db-after tx-report)]
                  (when-let [cid (commit-id-of db)]
                    (let [snap-id (str cid)
                          branch (name (get-in db [:config :branch]))]
                      (on-commit-fn {:type :commit
                                     :snapshot-id snap-id
                                     :branch branch
                                     :timestamp (System/currentTimeMillis)}))))))
    listener-key))

(defmethod hooks/remove-commit-hook! :datahike
  [_workspace system hook-id]
  (d/unlisten (:conn system) hook-id))
