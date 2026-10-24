(ns yggdrasil.adapters.overlay-parent-lifetime-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.connections :as connections]
            [datahike.store :as store]
            [datahike.versioning :as dv]
            [geschichte.repo :as repo]
            [yggdrasil.adapters.datahike :as dh]
            [yggdrasil.adapters.datahike-cleanup :as cleanup]
            [yggdrasil.adapters.geschichte :as gy]
            [yggdrasil.convergent.overlay :as overlay]
            [yggdrasil.protocols :as p]))

(defonce ^:private retained-fixtures (atom {}))
(declare conn-count)

(defn- with-source [kind f]
  (when (seq @retained-fixtures)
    (throw (ex-info "Prior native fixture is retained; reconcile or restart before retrying" {})))
  (let [cfg {:store {:backend :memory :id (random-uuid)} :keep-history? true
             :schema-flexibility :write :commit-graph? true}
        id (get-in cfg [:store :id])
        track! (fn [overlay]
                 (swap! retained-fixtures update-in [id :overlays] conj overlay)
                 overlay)]
    ;; Native fixture ownership, not forkable application state. Keep exact
    ;; acquisitions/failure locators until every owned cleanup is acknowledged.
    (swap! retained-fixtures assoc id {:config cfg :stage :creating :overlays []})
    (try
      (d/create-database cfg)
      (swap! retained-fixtures assoc-in [id :stage] :connecting)
      (let [conn (d/connect cfg)]
        (swap! retained-fixtures update id assoc :conn conn :stage :open)
        (let [system (case kind
                       :geschichte (do (repo/init! conn)
                                       (repo/write! conn "basis.txt" (.getBytes "Source" "UTF-8"))
                                       (repo/stage-all! conn)
                                       (repo/commit! conn {:message "Basis"})
                                       (gy/create conn))
                       :datahike (do (d/transact conn [{:db/ident :note/id :db/valueType :db.type/string
                                                        :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
                                     (d/transact conn [{:note/id "source"}])
                                     (dh/create conn)))]
          (f conn system track!))
        (when-not (and (= 1 (conn-count conn))
                       (every? #(= :closed (:cleanup-phase (cleanup/info (:cleanup-owner %))))
                               (get-in @retained-fixtures [id :overlays])))
          (throw (ex-info "Native fixture still has live or uncertain owners" {})))
        (d/release conn)
        (swap! retained-fixtures assoc-in [id :stage] :root-released)
        (d/delete-database cfg)
        (swap! retained-fixtures dissoc id))
      (catch Throwable error
        (swap! retained-fixtures update id assoc :failure error)
        (throw error)))))

(defn- conn-count [conn]
  (let [cfg (:config @conn)]
    (get-in @connections/*connections* [[(store/store-identity (:store cfg)) :db] :count])))

(deftest siblings-discard-after-their-source-has-merged-and-been-disposed
  (doseq [kind [:geschichte :datahike]
          historical? [false true]
          reverse? [false true]]
    (with-source kind
      (fn [conn system track!]
        (let [baseline (conn-count conn)
              opts (if historical? {:snapshot (p/snapshot-id system)} {})
              source (track! (p/overlay system opts))
              source-system (overlay/overlay-system source)
              a (track! (p/overlay source-system opts))
              b (track! (p/overlay source-system opts))
              [winner loser] (if reverse? [b a] [a b])]
          (p/merge-down! winner)
          (p/discard! winner)
          (p/merge-down! source)
          (p/discard! source)
          (p/discard! loser)
          (p/discard! loser)
          (is (= baseline (conn-count conn)) "Every canonical lease, including merge, balanced")
          (is (= #{:db} (d/branches conn)))
          (is (some? @conn))
          (case kind
            :geschichte (is (= "Source" (String. ^bytes (repo/read conn "basis.txt") "UTF-8")))
            :datahike (is (= ["source"] (d/q '[:find [?id ...] :where [_ :note/id ?id]] @conn)))))))))

(deftest nested-losers-and-an-unrelated-sibling-remain-disposable
  (doseq [kind [:geschichte :datahike]]
    (with-source kind
      (fn [conn system track!]
        (let [baseline (conn-count conn)
              source (track! (p/overlay system {}))
              unrelated (track! (p/overlay system {}))
              middle (track! (p/overlay (overlay/overlay-system source) {}))
              winner (track! (p/overlay (overlay/overlay-system middle) {}))
              loser (track! (p/overlay (overlay/overlay-system middle) {}))]
          (doseq [member [winner middle source]]
            (p/merge-down! member)
            (p/discard! member))
          (is (some? (p/snapshot-id (overlay/overlay-system unrelated))))
          (p/discard! loser)
          (p/discard! unrelated)
          (is (= baseline (conn-count conn)))
          (is (= #{:db} (d/branches conn))))))))

(deftest failed-branch-allocation-releases-the-acquired-canonical-lease
  ;; :refused throws before any write (a konserve write error); :registered
  ;; throws after the branch reached the registry (a lost acknowledgement).
  ;; Either outcome is observable through the owned canonical lease, so neither
  ;; may strand that lease or the branch.
  (doseq [kind [:geschichte :datahike]
          historical? [false true]
          fault [:refused :registered]]
    (with-source kind
      (fn [conn system _]
        (let [baseline (conn-count conn)
              before (d/branches conn)
              failure (ex-info "Injected konserve write error" {})
              branch! dv/branch!
              calls (atom 0)
              opts (if historical? {:snapshot (p/snapshot-id system)} {})
              error (with-redefs [dv/branch! (fn [& args]
                                               (swap! calls inc)
                                               (when (= :registered fault) (apply branch! args))
                                               (throw failure))]
                      (try (p/overlay system opts) nil
                           (catch Throwable error error)))]
          (is (= 1 @calls))
          (is (identical? failure error) "A resolved allocation failure is the native error")
          (is (= baseline (conn-count conn)) "The canonical cleanup lease is released")
          (is (= before (d/branches conn)) "No allocated branch is left behind"))))))
