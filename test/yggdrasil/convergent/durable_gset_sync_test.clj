(ns yggdrasil.convergent.durable-gset-sync-test
  "End-to-end: the REAL konserve-sync durable-CRDT walker over a REAL
   durable-gset store. Proves (a) konserve-sync's `crdt-walk-fn` understands the
   actual on-disk format durable-gset produces, (b) its ship-set agrees with
   yggdrasil's own reachability walk, and (c) the shipped nodes are sufficient
   to reconstruct the set on a subscriber's store. The transport itself
   (register/subscribe over kabel) is proven generic by konserve-sync's own
   pubsub tests; this validates the konserve-sync delegation for durable CRDTs.

   konserve-sync is a TEST-ONLY dependency — yggdrasil's runtime never requires
   it."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve-sync.walkers.crdt :as kcrdt]
            [yggdrasil.storage :as store]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-gset :as g]))

(defn- mem [] {:backend :memory :id (random-uuid)})

(defn- ship-keys!
  "Copy `ks` from `src` to `dst` (the work konserve-sync's transport does)."
  [src dst ks]
  (doseq [kk ks]
    (<!! (k/assoc dst kk (<!! (k/get src kk))))))

(deftest konserve-sync-walker-matches-durable-gset-and-reconstructs
  (let [peer-a (-> (g/durable-gset "kb" :store-config (mem))
                   (g/add :a1) (g/add :a2) (g/add :a3) g/flush!)
        kv-a   (:kv-store peer-a)
        root-a (get (d/load-roots kv-a) :main)]

    (testing "the real konserve-sync walker reaches every node + the pointers"
      (let [walk-set (<!! (kcrdt/crdt-walk-fn kv-a {}))]
        (is (contains? walk-set :crdt/roots))
        (is (contains? walk-set :crdt/freed))
        (is (contains? walk-set root-a) "the branch root is in the ship-set")
        (testing "and it AGREES with yggdrasil's own reachability walk"
          ;; walk-set = node addresses ∪ the two mutable pointer keywords
          (is (= (d/reachable-addresses kv-a root-a)
                 (disj walk-set :crdt/roots :crdt/freed))
              "konserve-sync's walker and durable/reachable-addresses agree"))))

    (testing "shipping the walk-set into a subscriber store reconstructs the set"
      (let [walk-set (<!! (kcrdt/crdt-walk-fn kv-a {}))
            sub      (<!! (new-mem-store))]
        (ship-keys! kv-a sub walk-set)
        ;; the subscriber restores A's head from the shipped nodes
        (let [sub-storage (store/create-storage sub {:content-addressed? true})
              restored    (d/restore-set compare root-a sub-storage)]
          (is (= #{:a1 :a2 :a3} (set restored))
              "the shipped nodes are sufficient to reconstruct A's set"))))))
