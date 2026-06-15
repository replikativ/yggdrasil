(ns yggdrasil.convergent.durable-orset-test
  "Durable OR-Set: add/observed-remove/re-add, add-wins under concurrency,
   cross-store convergence, durability, and the idempotent content-tag variant
   (the registry shape). VALUE SEMANTICS: mutators return new values; threaded."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-orset :as o]
            [yggdrasil.convergent.durable-2pset :as t]
            [yggdrasil.convergent.overlay :as ovl])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- mem [] {:backend :memory :id (random-uuid)})
(defn- tmpdir [] (str (Files/createTempDirectory "ygg-orset" (make-array FileAttribute 0))))

(deftest add-remove-readd
  (testing "remove actually removes (unlike a G-Set); re-add brings it back"
    (let [s (-> (o/durable-orset "reg" :store-config (mem)) (o/add :x) (o/add :y))]
      (is (= #{:x :y} (o/elements s)))
      (let [s (o/remove-elem s :x)]
        (is (= #{:y} (o/elements s)) "observed-remove drops :x")
        (let [s (o/add s :x)]
          (is (= #{:x :y} (o/elements s)) "re-add (fresh tag) brings :x back")
          (is (true? (c/-conflict-free? s)))
          (is (= :orset (p/system-type s))))))))

(deftest add-wins-concurrency
  (testing "a concurrent add the remover didn't observe SURVIVES the merge"
    (let [a (-> (o/durable-orset "reg" :store-config (mem)) (o/add :k))
          b (-> (o/durable-orset "reg" :store-config (mem)) (o/add :k))
          ;; a removes its observed :k; b's :k (different tag) is unobserved by a
          a (o/remove-elem a :k)]
      (is (= #{} (o/elements a)) "a removed its own :k")
      ;; converge: b's live add-tag for :k is not tombstoned ⇒ :k survives
      (let [a (o/merge-peer! a b)]
        (is (= #{:k} (o/elements a)) "add-wins: b's concurrent add survives a's remove")))))

(deftest cross-peer-converges
  (testing "two peers with disjoint add/remove ops converge to the same set"
    (let [a (-> (o/durable-orset "reg" :store-config (mem)) (o/add :a1) (o/add :shared))
          b (-> (o/durable-orset "reg" :store-config (mem)) (o/add :b1) (o/add :shared))
          b (o/remove-elem b :shared)           ; b removes its own observed :shared
          a (o/flush! (o/merge-peer! a b))
          b (o/flush! (o/merge-peer! b a))]
      ;; a still had a live :shared tag b never observed ⇒ add-wins keeps :shared
      (is (= (o/elements a) (o/elements b)) "strong eventual consistency")
      (is (= #{:a1 :b1 :shared} (o/elements a))))))

(deftest durable-across-reopen
  (testing "flush, reopen, restore both halves (adds + removals)"
    (let [sc {:backend :file :id (random-uuid) :path (tmpdir)}]
      (-> (o/durable-orset "reg" :store-config sc)
          (o/add :p) (o/add :q) (o/add :r) (o/remove-elem :q) o/flush!)
      (let [re (o/durable-orset "reg" :store-config sc)]
        (is (= #{:p :r} (o/elements re)) "removal persisted across reopen")))))

(deftest content-tag-idempotent-add
  (testing "with a content-hash tag-fn, re-adding the same element is a no-op
            (the registry shape — dedup by content)"
    (let [s (-> (o/durable-orset "reg" :store-config (mem) :tag-fn identity)
                (o/add :x) (o/add :x) (o/add :x))]
      (is (= #{:x} (o/elements s)))
      ;; only ONE [:x :x] pair exists (tag = element) ⇒ idempotent
      (is (= 1 (count (filter (fn [[e _]] (= e :x)) (seq (:adds s)))))
          "idempotent add: a single content-tagged pair"))))

(deftest orset-addressable-snapshot-freeze
  (testing "OR-Set snapshot-id (commit object) + as-of restores the frozen value"
    (let [s0  (-> (o/durable-orset "reg" :store-config (mem)) (o/add :a) (o/add :b))
          sid (p/snapshot-id s0)                           ; FREEZE {:a :b}
          s   (-> s0 (o/remove-elem :a) (o/add :c))]       ; evolve → {:b :c}
      (is (= #{:b :c} (o/elements s)))
      (is (= #{:a :b} (p/as-of s sid)) "as-of restores the frozen live elements"))))

(deftest twopset-addressable-snapshot-freeze
  (testing "2P-Set snapshot-id (commit object) + as-of restores the frozen value"
    (let [s0  (-> (t/durable-2pset "x" :store-config (mem)) (t/add :a) (t/add :b))
          sid (p/snapshot-id s0)                           ; FREEZE {:a :b}
          s   (t/remove-elem s0 :a)]                       ; evolve → {:b}
      (is (= #{:b} (t/elements s)))
      (is (= #{:a :b} (p/as-of s sid)) "as-of restores the frozen value")
      (is (= sid (p/snapshot-id (-> (t/durable-2pset "y" :store-config (mem)) (t/add :b) (t/add :a))))
          "content-addressed commit: equal halves (any order/store) → equal snapshot-id"))))

(deftest orset-overlay-isolate-merge-down
  (testing "OR-Set overlay isolates (the residue fix); merge-down! joins (add-wins)"
    (let [s  (-> (o/durable-orset "reg" :store-config (mem)) (o/add :a) (o/add :b))
          ov (p/overlay s {})]
      (ovl/overlay-swap! ov (fn [c] (o/add c :c)))
      (is (= #{:a :b :c} (o/elements (ovl/overlay-system ov))) "overlay evolves in isolation")
      (is (= #{:a :b}    (o/elements s))                     "parent untouched while overlay is open")
      (is (= #{:a :b :c} (o/elements (p/merge-down! ov))) "merge-down! joins the overlay"))))

(deftest twopset-overlay-isolate-merge-down
  (testing "2P-Set overlay isolates; merge-down! joins BOTH halves (remove propagates)"
    (let [s  (-> (t/durable-2pset "x" :store-config (mem)) (t/add :a) (t/add :b))
          ov (p/overlay s {})]
      (ovl/overlay-swap! ov (fn [c] (-> (t/add c :c) (t/remove-elem :a))))
      (is (= #{:b :c} (t/elements (ovl/overlay-system ov))) "overlay evolves in isolation")
      (is (= #{:a :b} (t/elements s))                       "parent untouched while overlay is open")
      (is (= #{:b :c} (t/elements (p/merge-down! ov)))
          "merge-down! unions adds+removals — the overlay's remove of :a propagates"))))

(deftest orset-store-layout-is-crdt-walkable
  (testing "both halves live under :crdt/roots — the shape konserve-sync's crdt
            walker syncs (the walk itself is covered in konserve-sync)"
    (let [s (-> (o/durable-orset "reg" :store-config (mem))
                (o/add :a) (o/add :b) (o/remove-elem :a) o/flush!)
          roots (d/load-roots (:kv-store s))]
      (is (contains? roots :adds))
      (is (contains? roots :removals)))))

(deftest twopset-delta-op-perspective
  (testing "2P-Set δ: add/remove-elem record {:adds}/{:removals} ops (no diffing);
            apply-delta integrates a peer's δ and converges == -join"
    (let [s  (-> (t/durable-2pset "a" :store-config {:backend :memory :id (random-uuid)})
                 (t/add :x) (t/add :y) (t/remove-elem :x))]
      (is (= {:adds #{:x :y} :removals #{:x}} (c/delta-of s)) "δ = the ops, accrued")
      (is (= #{:y} (t/elements s)) "live = adds − removals; δ in meta doesn't affect value")
      (let [peer   (-> (t/durable-2pset "b" :store-config {:backend :memory :id (random-uuid)}) (t/add :z))
            via-op (c/-apply-delta peer (c/delta-of s))]
        (is (= #{:y :z} (t/elements via-op)) "ops integrated: :x removed, :y + peer's :z live")
        (is (= (t/elements via-op) (t/elements (c/-join peer s))) "op-path ≡ state-path")))))

(deftest orset-delta-op-perspective
  (testing "OR-Set δ: add records {:adds #{[elem tag]}} ops; apply-delta ≡ -join"
    (let [o  (-> (o/durable-orset "a" :store-config {:backend :memory :id (random-uuid)})
                 (o/add :x) (o/add :y))
          dl (c/delta-of o)]
      (is (= #{:x :y} (set (map first (:adds dl)))) "δ :adds carries the [elem tag] add-pairs")
      (let [peer   (-> (o/durable-orset "b" :store-config {:backend :memory :id (random-uuid)}) (o/add :z))
            via-op (c/-apply-delta peer dl)]
        (is (= #{:x :y :z} (o/elements via-op)) "ops integrated")
        (is (= (o/elements via-op) (o/elements (c/-join peer o))) "op-path ≡ state-path")))))
