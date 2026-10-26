(ns yggdrasil.adapters.geschichte-checkpoint-async-test
  "Run the exact physical checkpoint path through real Node channel/CPS IO."
  (:require [cljs.test :refer-macros [is]]
            [datahike.api :as d]
            [datahike.versioning :as versioning]
            [geschichte.workspace :as workspace]
            [yggdrasil.adapters.datahike-cleanup :as cleanup]
            [geschichte.bytes :as bytes]
            [geschichte.query :as query]
            [geschichte.repo :as repo]
            [is.simm.partial-cps.core-async :as ca]
            [yggdrasil.adapters.geschichte :as gy]
            [yggdrasil.protocols :as p]
            [yggdrasil.test-async :refer-macros [deftest-async <?]]))

(defn- snapshot-proof [db]
  {:commit (d/commit-id db)
   :state (select-keys db [:config :hash :max-tx :max-eid :op-count :schema :secondary-index-keys])
   :refs (query/refs db) :worktree (query/worktree db) :stage (query/stage db)})

(deftest-async checkpoint-forks-earlier-dirty-state-and-cleans-up-on-node
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true :commit-graph? true :schema-flexibility :write}]
    (<? (ca/chan->cps (d/create-database cfg)))
    (let [conn (<? (ca/chan->cps (d/connect cfg {:sync? false})))
          child (atom nil)]
      (try
        (<? (repo/init! conn))
        (<? (repo/write! conn "state.txt" (bytes/utf8 "C1")))
        (<? (repo/stage-all! conn))
        (<? (repo/commit! conn {:message "C1"}))
        (<? (repo/write! conn "untracked.txt" (bytes/utf8 "Earlier dirty state")))
        (let [c1 @conn origin {:version 1 :kind :datahike :commit (d/commit-id c1)
                               :source {:store (get-in c1 [:config :store :id])
                                        :repository (:geschichte.repo/id (query/repository c1))}}
              system (gy/create conn)]
          (<? (repo/write! conn "state.txt" (bytes/utf8 "C2")))
          (<? (repo/stage-all! conn))
          (<? (repo/commit! conn {:message "C2"}))
          (<? (repo/write! conn "future.txt" (bytes/utf8 "Only C2")))
          (is (p/supports-checkpoint-overlay? system {:sync? false}))
          (is (not (p/supports-checkpoint-overlay? system {:sync? true})))
          (let [before (snapshot-proof @conn) original-owner cleanup/owner original-branch versioning/branch!
                original-fork workspace/fork! original-connect d/connect
                original-transact d/transact original-transact! d/transact!
                refuse (fn [& _] (throw (js/Error. "Validation acquired native resources")))]
            ;; Scope native spies through the actual suspended CPS operation,
            ;; then restore them before exercising ordinary construction below.
            (set! cleanup/owner refuse)
            (set! versioning/branch! refuse)
            (set! workspace/fork! refuse)
            (set! d/connect refuse)
            (set! d/transact refuse)
            (set! d/transact! refuse)
            (try
              (is (p/supports-checkpoint-validation? system {:sync? false}))
              (is (identical? origin (<? (p/validate-checkpoint-origin! system origin {:sync? false}))))
              (is (= before (snapshot-proof @conn)))
              (let [foreign (assoc-in origin [:source :store] (random-uuid))
                    failure (try (<? (p/validate-checkpoint-origin! system foreign {:sync? false})) nil
                                 (catch :default error error))]
                (is (= :yggdrasil.adapters.geschichte/checkpoint-unavailable (:type (ex-data failure)))))
              (finally
                (set! cleanup/owner original-owner)
                (set! versioning/branch! original-branch)
                (set! workspace/fork! original-fork)
                (set! d/connect original-connect)
                (set! d/transact original-transact)
                (set! d/transact! original-transact!))))
          (reset! child (<? (p/overlay-at-checkpoint system origin {:sync? false})))
          (let [selected (gy/connection @child)
                state (<? (repo/read selected "state.txt"))
                untracked (<? (repo/read selected "untracked.txt"))
                future (<? (repo/read selected "future.txt"))
                parent-state (<? (repo/read conn "state.txt"))]
            (is (= "C1" (bytes/decode-utf8 state)))
            (is (= "Earlier dirty state" (bytes/decode-utf8 untracked)))
            (is (nil? future))
            (is (= (query/worktree c1) (query/worktree @selected)))
            (is (= "C2" (bytes/decode-utf8 parent-state)))
            (<? (repo/write! selected "own.txt" (bytes/utf8 "Child only")))
            (let [parent-own (<? (repo/read conn "own.txt"))]
              (is (nil? parent-own)))))
        (finally
          (when @child (<? (p/discard! @child)))
          (d/release conn)
          (<? (ca/chan->cps (d/delete-database cfg))))))))
