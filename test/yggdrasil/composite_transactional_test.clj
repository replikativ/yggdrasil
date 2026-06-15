(ns yggdrasil.composite-transactional-test
  "A composite merge is transactional by default: `commit!` flushes every
   sub-system durable, then writes `:composite/root` LAST. So sub-state becomes
   durable atomically with the composite snapshot — a reader (resolving through
   `:composite/root`) never sees a half-merge, and a crash before the root write
   leaves the previous committed composite as the latest."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [yggdrasil.protocols :as p]
            [yggdrasil.composite :as cmp]
            [yggdrasil.convergent.durable-gset :as g])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- file-cfg [] {:backend :file :id (random-uuid)
                    :path (str (Files/createTempDirectory "ygg-comp" (make-array FileAttribute 0)))})

(deftest commit-flushes-all-subs-then-root
  (testing "sub-systems are durable only after the composite commits"
    (let [sc-a (file-cfg) sc-b (file-cfg) sc-comp (file-cfg)
          a (-> (g/durable-gset "a" :store-config sc-a) (g/add :a1) (g/add :a2))
          b (-> (g/durable-gset "b" :store-config sc-b) (g/add :b1))
          comp (cmp/composite [a b] :store-config sc-comp)]
      ;; pre-commit: the sub edits are in-memory only — a fresh reopen sees nothing
      (is (empty? (g/elements (g/durable-gset "a" :store-config sc-a))))
      (is (empty? (g/elements (g/durable-gset "b" :store-config sc-b))))
      ;; commit the composite → flushes every sub, then writes :composite/root
      (let [committed (p/commit! comp "snapshot-1")]
        (is (some? (p/snapshot-id committed)))
        ;; post-commit: every sub is durable (reopen each store → merged state)
        (is (= #{:a1 :a2} (g/elements (g/durable-gset "a" :store-config sc-a))))
        (is (= #{:b1} (g/elements (g/durable-gset "b" :store-config sc-b))))
        ;; the composite root names a bundle referencing both sub snapshots
        (let [root (k/get (:kv-store committed) :composite/root nil {:sync? true})]
          (is (some? root) ":composite/root advanced"))
        (let [meta (p/snapshot-meta committed (p/snapshot-id committed))]
          (is (= #{"a" "b"} (set (keys (:sub-snapshots meta))))
              "the committed bundle references every sub-system"))))))

(deftest branch-from-composite-snapshot-freezes-and-isolates
  (testing "branch! from a composite snapshot-id forks every sub at ITS recorded
            sub-snapshot — the whole composite frozen at a version + isolated"
    (let [sc     (file-cfg)
          opener (fn [id] (fn [kv o] (g/durable-gset id :kv-store kv
                                                     :roots-key [:crdt/roots id]
                                                     :sync? (:sync? o))))
          comp   (-> (cmp/composite [(opener "a")] :store-config sc)
                     (cmp/update-subsystem "a" #(g/add % :x))
                     (cmp/update-subsystem "a" #(g/add % :y)))]
      (let [comp (p/commit! comp "v1")
            sid  (p/snapshot-id comp)               ; FREEZE the composite at a:{:x :y}
            comp (p/commit! (cmp/update-subsystem comp "a" #(g/add % :z)) "v2")] ; → a:{:x :y :z}
        (let [frozen (-> comp (p/branch! :iso sid) (p/checkout :iso))
              fa0    (cmp/get-subsystem frozen "a")]
          (is (= #{:x :y} (g/elements fa0)) "sub a is frozen at the composite snapshot")
          (let [fa (g/add fa0 :w)]
            (is (= #{:x :y :w} (g/elements fa)) "the isolated branch evolves independently")
            (is (= #{:x :y :z} (g/elements (cmp/get-subsystem comp "a"))) "the live composite is untouched")))))))

(deftest composite-overlay-isolate-and-merge-down
  (testing "composite overlay = per-sub overlays; mutate the sub clones in
            isolation; merge-down! joins every sub back into the PARENT composite
            (store + index preserved)"
    (let [sc     (file-cfg)
          opener (fn [id] (fn [kv o] (g/durable-gset id :kv-store kv
                                                     :roots-key [:crdt/roots id]
                                                     :sync? (:sync? o))))
          comp   (-> (cmp/composite [(opener "a") (opener "b")] :store-config sc)
                     (cmp/update-subsystem "a" #(g/add % :x))
                     (cmp/update-subsystem "b" #(g/add % :y)))]
      (let [ov (p/overlay comp {})]
        (cmp/overlay-subsystem-swap! ov "a" #(g/add % :x2))
        (cmp/overlay-subsystem-swap! ov "b" #(g/add % :y2))
        (is (= #{:x :x2} (g/elements (cmp/overlay-subsystem ov "a"))) "sub a's overlay evolves in isolation")
        (is (= #{:x} (g/elements (cmp/get-subsystem comp "a"))) "parent sub a untouched while open")
        (let [merged (p/merge-down! ov)]
          (is (= #{:x :x2} (g/elements (cmp/get-subsystem merged "a"))) "merge-down! joined sub a")
          (is (= #{:y :y2} (g/elements (cmp/get-subsystem merged "b"))) "merge-down! joined sub b")
          (is (some? (p/snapshot-id merged)) "merged composite still resolves — store/index preserved"))))))

(deftest shared-store-unified-gc
  (testing "a co-located composite sweeps ONCE over the union of all roots —
            reclaims the orphaned superseded trees, keeps live state, and the
            composite still resolves afterwards"
    (let [sc     (file-cfg)
          opener (fn [id] (fn [kv o] (g/durable-gset id :kv-store kv
                                                     :roots-key [:crdt/roots id]
                                                     :sync? (:sync? o))))
          comp   (-> (cmp/composite [(opener "a")] :store-config sc)
                     (cmp/update-subsystem "a" #(g/add % :x))
                     (cmp/update-subsystem "a" #(g/add % :y)))
          comp   (p/commit! comp "c1")
          comp   (cmp/update-subsystem comp "a" #(g/add % :z))]
      (let [comp   (p/commit! comp "c2")          ; supersedes c1's index + sub trees
            before (count (k/keys (:kv-store comp) {:sync? true}))
            report (p/gc-sweep! comp nil)
            after  (count (k/keys (:kv-store comp) {:sync? true}))]
        (is (seq (:deleted report)) "reclaimed nodes from the superseded trees")
        (is (< after before) "the shared store shrank")
        ;; live state survives + the composite still resolves through :composite/root
        (is (= #{:x :y :z} (g/elements (cmp/get-subsystem comp "a"))) "live sub state intact")
        (let [sid (p/snapshot-id comp)]
          (is (some? (p/snapshot-meta comp sid)) "history still resolves")
          (is (= #{:x :y :z} (get (p/as-of comp sid) "a")) "as-of resolves the live sub"))))))

(deftest merge-commits-transactionally
  (testing "composite merge! delegates to the transactional commit"
    (let [a (-> (g/durable-gset "a" :store-config (file-cfg)) (g/add :x))
          b (-> (g/durable-gset "b" :store-config (file-cfg)) (g/add :y))
          comp (cmp/composite [a b] :store-config (file-cfg))
          ;; merging :main into :main is a no-op union, but it must still commit
          merged (p/merge! comp :main {})]
      (is (some? (p/snapshot-id merged)))
      (is (some? (p/snapshot-meta merged (p/snapshot-id merged)))
          "merge produced a committed composite snapshot"))))
