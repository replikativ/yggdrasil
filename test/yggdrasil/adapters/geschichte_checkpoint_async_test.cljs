(ns yggdrasil.adapters.geschichte-checkpoint-async-test
  "Run the exact physical checkpoint path through real Node channel/CPS IO."
  (:require [cljs.test :refer-macros [is]]
            [datahike.api :as d]
            [geschichte.bytes :as bytes]
            [geschichte.query :as query]
            [geschichte.repo :as repo]
            [is.simm.partial-cps.core-async :as ca]
            [yggdrasil.adapters.geschichte :as gy]
            [yggdrasil.protocols :as p]
            [yggdrasil.test-async :refer-macros [deftest-async <?]]))

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
