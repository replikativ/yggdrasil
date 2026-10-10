(ns yggdrasil.convergent.durable-flush-test
  "A flush writes its child nodes as ONE ordered multi-assoc on a store that applies
   batches atomically, and as ordered single writes on any other store."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [yggdrasil.test-async :refer [file-cfg]]
            [yggdrasil.convergent.gset :as g]))

(defn- flush-events
  "Add `n` elements to a fresh gset over `store-config`, flush, and return the write-hook
   events of that flush, plus the gset reopened from the same store."
  [store-config n]
  (let [gs (g/gset "kb" {:store-config store-config} {:sync? true})
        ;; a run of unflushed ops, then one flush: the bulk path
        gs (reduce #(g/conj %1 %2 {:sync? true :flush? false}) gs (range n))
        kv (:kv-store gs)
        events (atom [])]
    (k/add-write-hook! kv ::rec #(swap! events conj %))
    (let [gs (g/flush! gs {:sync? true})]
      (k/remove-write-hook! kv ::rec)
      {:events @events
       :reopened (g/gset "kb" {:kv-store kv} {:sync? true})
       :gs gs})))

(deftest atomic-store-flushes-children-as-one-batch
  (let [{:keys [events reopened]} (flush-events {:backend :memory :id (random-uuid)} 2000)
        batches (filter #(= :multi-assoc (:api-op %)) events)
        node-assocs (filter #(and (= :assoc (:api-op %)) (uuid? (:key %))) events)]
    (is (= 1 (count batches)) "one batch for all child nodes")
    (is (< 1 (count (:kvs (first batches)))) "a multi-node tree")
    (is (empty? node-assocs) "no node written on its own")
    (is (every? #(get-in (first batches) [:meta % :immutable?])
                (map first (:kvs (first batches))))
        "every node marked immutable")
    (is (= (set (range 2000)) (g/elements reopened)) "the flushed set reads back")))

(deftest other-stores-flush-children-one-by-one
  (let [{:keys [events reopened]} (flush-events (file-cfg) 2000)]
    (is (empty? (filter #(= :multi-assoc (:api-op %)) events)))
    (is (< 1 (count (filter #(and (= :assoc (:api-op %)) (:immutable? (:meta %))) events))))
    (is (= (set (range 2000)) (g/elements reopened)))))
