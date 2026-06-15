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
;; gc! is SAFE BY DEFAULT (reclaims nothing); pass a cutoff of `now` to reclaim
;; every superseded node up to this instant (the explicit "reclaim now" window).
;; Offset 1s ahead so a fast test's just-written orphans (same-millisecond
;; last-write) fall strictly before the cutoff; reachable nodes are spared by the
;; whitelist regardless of timestamp.
(defn- now [] (java.util.Date. (+ (System/currentTimeMillis) 1000)))

(deftest gc-safe-default-reclaims-nothing
  (testing "gc! with NO window reclaims NOTHING (never sweeps a node an in-flight
            lazy read might hold); a `:grace-period-ms 0` / `now` cutoff reclaims orphans"
    (let [gs (-> (g/durable-gset "t" :store-config (mem)) (g/add :a) g/flush! (g/add :b) g/flush!)
          kv (:kv-store gs)
          roots (vals (d/load-roots kv))]
      (is (empty? (d/gc! kv roots)) "default (epoch cutoff) ⇒ reclaim nothing")
      (is (empty? (d/gc! kv roots {:sync? true :grace-period-ms 600000}))
          "a 10-min grace still spares the just-written orphans")
      (is (pos? (count (d/gc! kv roots {:sync? true :grace-period-ms 0})))
          ":grace-period-ms 0 reclaims orphans older than now"))))

(deftest gset-gc-reclaims-superseded-nodes
  (testing "many flushes accumulate superseded trees; gc! reclaims them"
    (let [;; 6 flush generations → 6 superseded root trees pile up (value-semantic:
          ;; thread gs through each batch's adds + flush)
          gs (reduce (fn [gs batch] (g/flush! (reduce g/add gs batch)))
                     (g/durable-gset "t" :store-config (mem))
                     (partition-all 50 (range 300)))]
      (let [kv (:kv-store gs)
            before (node-key-count kv)
            live-before (g/elements gs)
            deleted (g/gc! gs {:remove-before (now)})
            after (node-key-count kv)]
        (is (pos? (count deleted)) "GC deleted superseded nodes")
        (is (< after before) "node count dropped")
        (is (= live-before (g/elements gs)) "live set unchanged after GC")
        (is (= (set (range 300)) (g/elements gs)))
        (testing "GC is idempotent — a second sweep on the quiescent store deletes nothing"
          (is (empty? (g/gc! gs {:remove-before (now)}))))
        (testing "every node reachable from the current root survived"
          (let [root (get (d/load-roots kv) :main)]
            (is (every? #(some? (k/get kv % {:sync? true}))
                        (d/reachable-addresses kv root)))))))))

(deftest gc-retains-held-snapshot
  (testing "a snapshot-id passed to gc-sweep! survives GC — as-of still resolves it"
    (let [gs0  (-> (g/durable-gset "t" :store-config (mem)) (g/add :a) (g/add :b) g/flush!)
          snap (p/snapshot-id gs0)                  ; S0 names the {:a :b} root
          ;; supersede S0's root tree twice, so S0's nodes are non-live
          gs   (-> gs0 (g/add :c) g/flush! (g/add :d) g/flush!)]
      ;; GC pinning S0: its now-unreferenced nodes must be RETAINED (else as-of
      ;; below would read a swept node). Guards the gc-sweep! :retain-roots wiring.
      (p/gc-sweep! gs #{snap} {:remove-before (now)})
      (is (= #{:a :b} (p/as-of gs snap)) "held snapshot survived GC (retention works)")
      (is (= #{:a :b :c :d} (g/elements gs)) "live set intact after GC"))))

(deftest two-pset-gc-keeps-both-halves
  (testing "2P-Set GC keeps adds + removals roots (tombstones are live members)"
    (let [s0 (reduce (fn [s batch] (d2p/flush! (reduce d2p/add s batch)))
                     (d2p/durable-2pset "t" :store-config (mem) :comparator compare)
                     (partition-all 30 (range 150)))
          s  (d2p/flush! (-> s0 (d2p/remove-elem 7) (d2p/remove-elem 42)))]
      (let [live-before (d2p/elements s)
            deleted (d2p/gc! s {:remove-before (now)})]
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
            deleted (reg/gc! r {:remove-before (now)})
            after (node-key-count kv)]
        (is (pos? (count deleted)))
        (is (<= after before))
        (is (= n-before (reg/entry-count r)) "all registered entries still live"))
      (reg/close! r))))
