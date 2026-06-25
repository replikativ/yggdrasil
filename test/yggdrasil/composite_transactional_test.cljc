(ns yggdrasil.composite-transactional-test
  "A composite merge is transactional by default: `commit!` flushes every
   sub-system durable, then writes `:composite/root` LAST. So sub-state becomes
   durable atomically with the composite snapshot — a reader (resolving through
   `:composite/root`) never sees a half-merge, and a crash before the root write
   leaves the previous committed composite as the latest.

   PORTABLE — file-backed on BOTH platforms (JVM file store + konserve node
   filestore). Sub-touching composite methods are async (`<?`); in-memory history
   reads (`snapshot-meta`) stay sync. `get-subsystem`/`branch!`/`checkout`/
   `overlay`/`overlay-subsystem` are sync."
  (:require [clojure.test :refer [is testing]]
            [yggdrasil.test-async :refer [deftest-async <? sync? file-cfg]]
            [yggdrasil.kbridge :as kb]
            [konserve.core :as k]
            [yggdrasil.protocols :as p]
            [yggdrasil.composite :as cmp]
            [yggdrasil.convergent.gset :as g]
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [is.simm.partial-cps.async :refer [async]])))

;; an opener co-locating a durable G-Set in the composite's store under its own cell
(defn- opener [id]
  (fn [kv o] (g/gset id {:kv-store kv :roots-key [:crdt/roots id]} {:sync? (:sync? o)})))

(defn- future-date []
  #?(:clj  (java.util.Date. (+ (System/currentTimeMillis) 1000))
     :cljs (js/Date. (+ (.getTime (js/Date.)) 1000))))

(deftest-async commit-flushes-all-subs-then-root
  (testing "sub-systems are durable only after the composite commits"
    (let [sc-a (file-cfg) sc-b (file-cfg) sc-comp (file-cfg)
          a (<? (g/gset "a" {:store-config sc-a} {:sync? sync?}))
          a (<? (g/conj a :a1)) a (<? (g/conj a :a2))
          b (<? (g/gset "b" {:store-config sc-b} {:sync? sync?}))
          b (<? (g/conj b :b1))
          comp (<? (cmp/composite [a b] {:store-config sc-comp} {:sync? sync?}))]
      ;; pre-commit: the sub edits are in-memory only — a fresh reopen sees nothing
      (is (empty? (<? (g/elements (<? (g/gset "a" {:store-config sc-a} {:sync? sync?}))))))
      (is (empty? (<? (g/elements (<? (g/gset "b" {:store-config sc-b} {:sync? sync?}))))))
      (let [committed (<? (p/commit! comp "snapshot-1"))
            sid       (<? (p/snapshot-id committed))]
        (is (some? sid))
        ;; post-commit: every sub is durable (reopen each store → merged state)
        (is (= #{:a1 :a2} (<? (g/elements (<? (g/gset "a" {:store-config sc-a} {:sync? sync?}))))))
        (is (= #{:b1} (<? (g/elements (<? (g/gset "b" {:store-config sc-b} {:sync? sync?}))))))
        (is (some? (<? (kb/k-get (:kv-store committed) :composite/root {:sync? sync?}))) ":composite/root advanced")
        (let [meta (p/snapshot-meta committed sid)]
          (is (= #{"a" "b"} (set (keys (:sub-snapshots meta))))
              "the committed bundle references every sub-system"))))))

(deftest-async branch-from-composite-snapshot-freezes-and-isolates
  (testing "branch! from a composite snapshot-id forks every sub at ITS recorded
            sub-snapshot — the whole composite frozen at a version + isolated"
    (let [comp (<? (cmp/composite [(opener "a")] {:store-config (file-cfg)} {:sync? sync?}))
          comp (<? (cmp/update-subsystem comp "a" #(g/conj % :x)))
          comp (<? (cmp/update-subsystem comp "a" #(g/conj % :y)))
          comp (<? (p/commit! comp "v1"))
          sid  (<? (p/snapshot-id comp))                ; FREEZE the composite at a:{:x :y}
          comp (<? (p/commit! (<? (cmp/update-subsystem comp "a" #(g/conj % :z))) "v2"))] ; → a:{:x :y :z}
      (let [frozen (-> comp (p/branch! :iso sid) (p/checkout :iso))
            fa0    (cmp/get-subsystem frozen "a")]
        (is (= #{:x :y} (<? (g/elements fa0))) "sub a is frozen at the composite snapshot")
        (let [fa (<? (g/conj fa0 :w))]
          (is (= #{:x :y :w} (<? (g/elements fa))) "the isolated branch evolves independently")
          (is (= #{:x :y :z} (<? (g/elements (cmp/get-subsystem comp "a")))) "the live composite is untouched"))))))

(deftest-async composite-overlay-isolate-and-merge-down
  (testing "composite overlay = per-sub overlays; mutate the sub clones in
            isolation; merge-down! joins every sub back into the PARENT composite"
    (let [comp (<? (cmp/composite [(opener "a") (opener "b")] {:store-config (file-cfg)} {:sync? sync?}))
          comp (<? (cmp/update-subsystem comp "a" #(g/conj % :x)))
          comp (<? (cmp/update-subsystem comp "b" #(g/conj % :y)))
          ov   (p/overlay comp {})]
      (<? (cmp/overlay-subsystem-swap! ov "a" #(g/conj % :x2)))
      (<? (cmp/overlay-subsystem-swap! ov "b" #(g/conj % :y2)))
      (is (= #{:x :x2} (<? (g/elements (cmp/overlay-subsystem ov "a")))) "sub a's overlay evolves in isolation")
      (is (= #{:x} (<? (g/elements (cmp/get-subsystem comp "a")))) "parent sub a untouched while open")
      (let [merged (<? (p/merge-down! ov))]
        (is (= #{:x :x2} (<? (g/elements (cmp/get-subsystem merged "a")))) "merge-down! joined sub a")
        (is (= #{:y :y2} (<? (g/elements (cmp/get-subsystem merged "b")))) "merge-down! joined sub b")
        (is (some? (<? (p/snapshot-id merged))) "merged composite still resolves — store/index preserved")))))

(deftest-async shared-store-unified-gc
  (testing "a co-located composite sweeps ONCE over the union of all roots —
            reclaims the orphaned superseded trees, keeps live state, and resolves"
    (let [comp (<? (cmp/composite [(opener "a")] {:store-config (file-cfg)} {:sync? sync?}))
          comp (<? (cmp/update-subsystem comp "a" #(g/conj % :x)))
          comp (<? (cmp/update-subsystem comp "a" #(g/conj % :y)))
          comp (<? (p/commit! comp "c1"))
          comp (<? (cmp/update-subsystem comp "a" #(g/conj % :z)))
          comp (<? (p/commit! comp "c2"))              ; supersedes c1's index + sub trees
          before (count (<? (kb/sync-or-cps (k/keys (:kv-store comp) {:sync? sync?}) {:sync? sync?})))
          ;; cutoff 1s ahead = "reclaim every orphan up to now" (reachable nodes are
          ;; spared by the whitelist, not the timestamp; a bare `now` can collide).
          report (<? (p/gc-sweep! comp nil {:remove-before (future-date) :sync? sync?}))
          after  (count (<? (kb/sync-or-cps (k/keys (:kv-store comp) {:sync? sync?}) {:sync? sync?})))]
      (is (seq (:deleted report)) "reclaimed nodes from the superseded trees")
      (is (< after before) "the shared store shrank")
      (is (= #{:x :y :z} (<? (g/elements (cmp/get-subsystem comp "a")))) "live sub state intact")
      (let [sid (<? (p/snapshot-id comp))]
        (is (some? (p/snapshot-meta comp sid)) "history still resolves")
        (is (= #{:x :y :z} (get (<? (p/as-of comp sid)) "a")) "as-of resolves the live sub")))))

(deftest-async merge-commits-transactionally
  (testing "composite merge! delegates to the transactional commit"
    (let [a (<? (g/gset "a" {:store-config (file-cfg)} {:sync? sync?}))
          a (<? (g/conj a :x))
          b (<? (g/gset "b" {:store-config (file-cfg)} {:sync? sync?}))
          b (<? (g/conj b :y))
          comp (<? (cmp/composite [a b] {:store-config (file-cfg)} {:sync? sync?}))
          merged (<? (p/merge! comp :main {}))
          sid (<? (p/snapshot-id merged))]
      (is (some? sid))
      (is (some? (p/snapshot-meta merged sid)) "merge produced a committed composite snapshot"))))
