(ns yggdrasil.convergent.durable-roots-test
  "Convergent root cell: the :crdt/roots branch→root map is a grow-map (merge on
   write), and merge-peer! converges EVERY branch — so two peers that each add a
   different branch don't lose one under blind LWW."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-gset :as g]))

(defn- mem [] {:backend :memory :id (random-uuid)})

(deftest save-roots-is-a-grow-map
  (testing "save-roots! merges, never clobbers a branch it doesn't know"
    (let [kv (:kv-store (g/durable-gset "t" :store-config (mem)))]
      (d/save-roots! kv {:main :ra})
      (d/save-roots! kv {:feature :rf})        ; a writer that only knows :feature
      (is (= {:main :ra :feature :rf} (d/load-roots kv)) ":main survived")
      (d/save-roots! kv {:main :ra2})          ; shared branch: incoming wins
      (is (= {:main :ra2 :feature :rf} (d/load-roots kv))))))

(deftest multi-branch-peers-converge
  (testing "two peers each add a distinct branch → merge keeps BOTH branches"
    (let [a (-> (g/durable-gset "kb" :store-config (mem))
                (g/add :shared)
                (p/branch! :fa) (p/checkout :fa) (g/add :a-only) g/flush!)
          b (-> (g/durable-gset "kb" :store-config (mem))
                (g/add :shared)
                (p/branch! :fb) (p/checkout :fb) (g/add :b-only) g/flush!)]
      ;; converge both directions
      (g/merge-peer! a b) (g/flush! a)
      (g/merge-peer! b a) (g/flush! b)
      ;; every branch present on both peers (none lost to LWW)
      (is (= #{:main :fa :fb} (p/branches a)))
      (is (= #{:main :fa :fb} (p/branches b)))
      (is (= (p/branches a) (p/branches b)))
      ;; the branch-only elements survived on both
      (is (= #{:shared :a-only} (g/elements (p/checkout a :fa))))
      (is (= #{:shared :b-only} (g/elements (p/checkout b :fb))))
      ;; and the roots cell on disk reflects all three branches
      (is (= #{:main :fa :fb} (set (keys (d/load-roots (:kv-store a)))))))))
