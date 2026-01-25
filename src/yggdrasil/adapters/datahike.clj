(ns yggdrasil.adapters.datahike
  "Datahike adapter for Yggdrasil protocols.

  Wraps a Datahike connection (atom of db state) and exposes
  Snapshotable, Branchable, Graphable, and Mergeable.

  Requires datahike on the classpath — this is a bridge, not a dependency."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [konserve.core :as k]
            [superv.async :refer [<? S go-loop-try]]
            [clojure.core.async :as async]))

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

(defn- stored->db-fn []
  (requiring-resolve 'datahike.writing/stored->db))

(defn- stored-db?-fn []
  (requiring-resolve 'datahike.writing/stored-db?))

(defn- branch!-fn []
  (requiring-resolve 'datahike.experimental.versioning/branch!))

(defn- delete-branch!-fn []
  (requiring-resolve 'datahike.experimental.versioning/delete-branch!))

(defn- merge!-fn []
  (requiring-resolve 'datahike.experimental.versioning/merge!))

(defn- commit-as-db-fn []
  (requiring-resolve 'datahike.experimental.versioning/commit-as-db))

;; ============================================================
;; History traversal (synchronous, bounded)
;; ============================================================

(defn- walk-history
  "Walk commit graph from starting refs, collecting snapshot-ids.
   Returns vector of commit UUIDs in traversal order."
  [store start-refs {:keys [limit] :or {limit 100}}]
  (let [stored->db (stored->db-fn)]
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
              (let [db (stored->db raw-db store)
                    parents (parent-ids-of db)]
                (recur (into (vec rest) parents)
                       (conj visited current)
                       (conj result (commit-id-of db))))
              (recur (vec rest) (conj visited current) result))))))))

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
    (t/->Capabilities true true true true false false))

  p/Snapshotable
  (snapshot-id [_]
    (str (commit-id-of (db-of conn))))

  (parent-ids [_]
    (let [parents (parent-ids-of (db-of conn))]
      (set (map str parents))))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (let [store (store-of conn)
          commit-as-db (commit-as-db-fn)
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (commit-as-db store uuid)))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [store (store-of conn)
          commit-as-db (commit-as-db-fn)
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (when-let [db (commit-as-db store uuid)]
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

  (branch! [_ name]
    (let [branch! (branch!-fn)]
      (branch! conn (branch-of conn) name)))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [_ name from _opts]
    (let [branch! (branch!-fn)]
      (branch! conn from name)))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [_ name _opts]
    (let [delete-branch! (delete-branch!-fn)]
      (delete-branch! conn name)))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this _name _opts]
    ;; In Datahike, "checkout" means connecting to a different branch.
    ;; We return a new DatahikeSystem pointing to that branch's head.
    ;; The caller is responsible for managing the connection lifecycle.
    this)

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
          ancestors-a (set (walk-history store [uuid-a] {:limit nil}))
          ;; Walk b's history until we find something in a's ancestors
          commit-as-db (commit-as-db-fn)]
      (loop [queue [uuid-b]
             visited #{}]
        (when (seq queue)
          (let [[current & rest] queue]
            (if (visited current)
              (recur (vec rest) visited)
              (if (ancestors-a (str current))
                (str current)
                (if-let [db (commit-as-db store current)]
                  (recur (into (vec rest) (parent-ids-of db))
                         (conj visited current))
                  (recur (vec rest) (conj visited current))))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [this _opts]
    (let [store (store-of conn)
          branches (p/branches this)
          all-ids (walk-history store (vec branches) {:limit nil})
          commit-as-db (commit-as-db-fn)]
      {:nodes (into {}
                    (for [id all-ids
                          :let [uuid (parse-uuid id)
                                db (when uuid (commit-as-db store uuid))]
                          :when db]
                      [id {:parent-ids (set (map str (parent-ids-of db)))
                           :meta (p/snapshot-meta this id)}]))
       :branches (into {}
                       (for [b branches]
                         [b (when-let [db (k/get store b nil {:sync? true})]
                              (str (commit-id-of ((stored->db-fn) db store))))]))
       :roots (set (filter
                    (fn [id]
                      (let [uuid (parse-uuid id)
                            db (when uuid (commit-as-db store uuid))]
                        (or (nil? db) (empty? (parent-ids-of db)))))
                    all-ids))}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (p/snapshot-meta this snap-id))

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [_ source opts]
    (let [merge! (merge!-fn)
          parents (if (keyword? source)
                    #{source}
                    #{(if (uuid? source) source (parse-uuid (str source)))})]
      (merge! conn parents (or (:tx-data opts) []) (:tx-meta opts))
      (str (commit-id-of (db-of conn)))))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b _opts]
    ;; Datahike doesn't have built-in conflict detection yet.
    ;; This would require diffing the two snapshots.
    [])

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    ;; Returns datoms that differ between two snapshots.
    ;; TODO: implement proper diff via index scan
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
