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
            [datahike.experimental.versioning :as dv]
            [datahike.writing :as dw]))

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

;; ============================================================
;; History traversal (synchronous, bounded)
;; ============================================================

(defn- walk-history
  "Walk commit graph from starting refs, collecting snapshot-ids.
   Returns vector of commit UUIDs in traversal order."
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
                     (conj result (commit-id-of db))))
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
                          (compute-branch-diff source-db target-db)))
                      [])]
      (dv/merge! conn parents tx-data (:tx-meta opts))
      this))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b _opts]
    [])

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    {:from a :to b :diff :not-implemented}))

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
