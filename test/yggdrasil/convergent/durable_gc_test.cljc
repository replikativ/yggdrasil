(ns yggdrasil.convergent.durable-gc-test
  "Mark-and-sweep GC for durable CRDTs (datahike-style: whitelist reachable nodes
   via the PSS walk, then konserve.gc/sweep! the rest). Each flush after a
   content-addressed mutation supersedes the old root tree; without GC those
   nodes accumulate forever. GC reclaims them while preserving the live set.

   PORTABLE — fixed-chain GC tests run synchronously on the JVM and as a partial-
   cps `async` block on cljs (every async durable op wrapped in `<?`). The BULK
   tests (reduce over many flush generations) and the registry-coordinator test
   are JVM-only: a sequential async fold over 300 elements / a sync registry
   facade don't translate to a single cljs `async` body without loop/recur."
  (:require [clojure.test :refer [is testing]]
            [yggdrasil.test-async :refer [deftest-async <? <gc sync? file-cfg]]
            [konserve.core :as k]
            [yggdrasil.types :as ytypes]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.gset :as g]
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime])
            #?(:clj [yggdrasil.convergent.twopset :as d2p])
            #?(:clj [yggdrasil.registry :as reg])
            #?(:clj [yggdrasil.types :as t]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <? <gc]]
                            [is.simm.partial-cps.async :refer [async]])))

;; gc! is SAFE BY DEFAULT (reclaims nothing); pass a cutoff of `now` to reclaim
;; every superseded node up to this instant (the explicit "reclaim now" window).
;; Offset 1s ahead so a fast test's just-written orphans (same-millisecond
;; last-write) fall strictly before the cutoff; reachable nodes are spared by the
;; whitelist regardless of timestamp.
(defn- now []
  (#?(:clj java.util.Date. :cljs js/Date.) (+ (ytypes/now-ms) 1000)))

(deftest-async gc-safe-default-reclaims-nothing
  (testing "gc! with NO window reclaims NOTHING (never sweeps a node an in-flight
            lazy read might hold); a `:grace-period-ms 0` / `now` cutoff reclaims orphans"
    ;; MULTI-NODE set: a single-leaf set stores no nodes under root fusion (leaf = inlined
    ;; root), so there'd be no orphans to reclaim. Build ~200 elements (batched, one flush)
    ;; so the tree has stored leaves, then a further flush supersedes some → real orphans.
    (let [gs0 (<? (g/gset "t" {:store-config (file-cfg)} {:sync? sync?}))
          gs1 (loop [gs gs0 xs (range 1000)]   ; ~1000 ⇒ a reliably MULTI-node MST tree
                (if (seq xs)
                  (recur (<? (g/conj gs (first xs) {:sync? sync? :flush? false})) (rest xs))
                  gs))
          gs2 (<? (g/flush! gs1))
          gs3 (<? (g/conj gs2 1000)) gs (<? (g/flush! gs3))
          kv (:kv-store gs)
          roots (d/head-roots (<? (d/load-head kv :main {} {:sync? sync?})))
          spare {:spare-keys (d/head-cell-keys {} #{:main})}]
      (is (empty? (<gc (d/gc! kv roots {} spare))) "default (epoch cutoff) ⇒ reclaim nothing")
      (is (empty? (<gc (d/gc! kv roots {} (merge spare {:grace-period-ms 600000}))))
          "a 10-min grace still spares the just-written orphans")
      (is (pos? (count (<gc (d/gc! kv roots {} (merge spare {:grace-period-ms 0})))))
          ":grace-period-ms 0 reclaims orphans older than now"))))

(deftest-async gc-retains-held-snapshot
  (testing "a snapshot-id passed to gc-sweep! survives GC — as-of still resolves it"
    (let [gs0  (<? (g/gset "t" {:store-config (file-cfg)} {:sync? sync?}))
          gs0  (<? (g/conj gs0 :a)) gs0 (<? (g/conj gs0 :b)) gs0 (<? (g/flush! gs0))
          snap (<? (p/snapshot-id gs0))                  ; S0 names the {:a :b} root
          ;; supersede S0's root tree twice, so S0's nodes are non-live
          gs   (<? (g/conj gs0 :c)) gs (<? (g/flush! gs))
          gs   (<? (g/conj gs :d))  gs (<? (g/flush! gs))]
      ;; GC pinning S0: its now-unreferenced nodes must be RETAINED (else as-of
      ;; below would read a swept node). Guards the gc-sweep! :retain-roots wiring.
      (<gc (p/gc-sweep! gs #{snap} {:remove-before (now)}))
      (is (= #{:a :b} (<? (p/as-of gs snap))) "held snapshot survived GC (retention works)")
      (is (= #{:a :b :c :d} (<? (g/elements gs))) "live set intact after GC"))))

;; ── JVM-only: BULK reduce-built GC tests + the registry-coordinator path. A
;; sequential async fold over hundreds of elements / a synchronous registry
;; facade don't translate to a single cljs `async` body without loop/recur. ────
#?(:clj
   (do
     (defn- raw-keys [kv] (map #(if (map? %) (:key %) %) (k/keys kv {:sync? true})))
     (defn- node-key-count [kv] (count (remove keyword? (raw-keys kv))))

     (clojure.test/deftest gset-gc-reclaims-superseded-nodes
       (testing "many flushes accumulate superseded trees; gc! reclaims them"
         (let [;; 6 flush generations → 6 superseded root trees pile up (value-semantic:
               ;; thread gs through each batch's adds + flush)
               gs (reduce (fn [gs batch] (g/flush! (reduce g/conj gs batch)))
                          (g/gset "t" {:store-config (file-cfg)})
                          (partition-all 50 (range 300)))]
           (let [kv (:kv-store gs)
                 before (node-key-count kv)
                 live-before (g/elements gs)
                 deleted (<gc (g/gc! gs {:remove-before (now)}))
                 after (node-key-count kv)]
             (is (pos? (count deleted)) "GC deleted superseded nodes")
             (is (< after before) "node count dropped")
             (is (= live-before (g/elements gs)) "live set unchanged after GC")
             (is (= (set (range 300)) (g/elements gs)))
             (testing "GC is idempotent — a second sweep on the quiescent store deletes nothing"
               (is (empty? (<gc (g/gc! gs {:remove-before (now)})))))
             (testing "every node reachable from the current root survived"
               ;; the head is a fused root NODE now → reachable-of walks its CHILDREN
               ;; (the root itself is inlined in the cell, not a store object).
               (let [root (:root (d/load-head kv :main))]
                 (is (every? #(some? (k/get kv % {:sync? true}))
                             (d/reachable-of kv root {:sync? true})))))))))

     (clojure.test/deftest two-pset-gc-keeps-both-halves
       (testing "2P-Set GC keeps adds + removals roots (tombstones are live members)"
         ;; ~1000 elements ⇒ the adds half is a reliably MULTI-node tree, so re-flush across
         ;; generations supersedes stored children → real orphans to reclaim (a single-leaf
         ;; half stores no nodes under fusion).
         (let [s0 (reduce (fn [s batch] (d2p/flush! (reduce d2p/conj s batch)))
                          (d2p/twopset "t" {:store-config (file-cfg) :comparator compare})
                          (partition-all 200 (range 1000)))
               s  (d2p/flush! (-> s0 (d2p/disj 7) (d2p/disj 42)))]
           (let [live-before (d2p/elements s)
                 deleted (<gc (d2p/gc! s {:remove-before (now)}))]
             (is (pos? (count deleted)))
             (is (= live-before (d2p/elements s)) "live (adds − removals) unchanged")
             (is (not (contains? (d2p/elements s) 7)) "tombstoned element stays removed")))))

     (clojure.test/deftest registry-gc-via-coordinator-path
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
                 deleted (<gc (reg/gc! r {:remove-before (now)}))
                 after (node-key-count kv)]
             (is (pos? (count deleted)))
             (is (<= after before))
             (is (= n-before (reg/entry-count r)) "all registered entries still live"))
           (reg/close! r))))))
