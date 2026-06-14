(ns yggdrasil.convergent.durable-gc-test
  "Mark-and-sweep GC for durable CRDTs (datahike-style: whitelist reachable nodes
   via the PSS walk, then konserve.gc/sweep! the rest). Each flush after a
   content-addressed mutation supersedes the old root tree; without GC those
   nodes accumulate forever. GC reclaims them while preserving the live set."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-gset :as g]
            [yggdrasil.convergent.durable-2pset :as d2p]
            [yggdrasil.registry :as reg]
            [yggdrasil.types :as t]))

(defn- mem [] {:backend :memory :id (random-uuid)})
(defn- raw-keys [kv] (map #(if (map? %) (:key %) %) (k/keys kv {:sync? true})))
(defn- node-key-count [kv] (count (remove keyword? (raw-keys kv))))

(deftest gset-gc-reclaims-superseded-nodes
  (testing "many flushes accumulate superseded trees; gc! reclaims them"
    (let [gs (g/durable-gset "t" :store-config (mem))]
      ;; 6 flush generations → 6 superseded root trees pile up
      (doseq [batch (partition-all 50 (range 300))]
        (doseq [i batch] (g/add gs i))
        (g/flush! gs))
      (let [kv (:kv-store gs)
            before (node-key-count kv)
            live-before (g/elements gs)
            deleted (g/gc! gs)
            after (node-key-count kv)]
        (is (pos? (count deleted)) "GC deleted superseded nodes")
        (is (< after before) "node count dropped")
        (is (= live-before (g/elements gs)) "live set unchanged after GC")
        (is (= (set (range 300)) (g/elements gs)))
        (testing "GC is idempotent — a second sweep on the quiescent store deletes nothing"
          (is (empty? (g/gc! gs))))
        (testing "every node reachable from the current root survived"
          (let [root (get (d/load-roots kv) :main)]
            (is (every? #(some? (k/get kv % {:sync? true}))
                        (d/reachable-addresses kv root)))))))))

(deftest two-pset-gc-keeps-both-halves
  (testing "2P-Set GC keeps adds + removals roots (tombstones are live members)"
    (let [s (d2p/durable-2pset "t" :store-config (mem) :comparator compare)]
      (doseq [batch (partition-all 30 (range 150))]
        (doseq [i batch] (d2p/add s i))
        (d2p/flush! s))
      (d2p/remove-elem s 7) (d2p/remove-elem s 42) (d2p/flush! s)
      (let [live-before (d2p/elements s)
            deleted (d2p/gc! s)]
        (is (pos? (count deleted)))
        (is (= live-before (d2p/elements s)) "live (adds − removals) unchanged")
        (is (not (contains? (d2p/elements s) 7)) "tombstoned element stays removed")))))

(deftest registry-gc-via-coordinator-path
  (testing "registry.gc! reclaims superseded index trees, entries preserved"
    (let [r (reg/create-registry)]
      (dotimes [i 120]
        (reg/register! r (t/->RegistryEntry (str "s" i) "git:a" "main"
                                            (t/->HLC (* i 100) 0) nil nil nil))
        (when (zero? (mod i 20)) (reg/flush! r)))
      (reg/flush! r)
      (let [kv (:kv-store r)
            before (node-key-count kv)
            n-before (reg/entry-count r)
            deleted (reg/gc! r)
            after (node-key-count kv)]
        (is (pos? (count deleted)))
        (is (<= after before))
        (is (= n-before (reg/entry-count r)) "all registered entries still live"))
      (reg/close! r))))
