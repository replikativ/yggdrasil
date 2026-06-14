(ns yggdrasil.convergent.durable-orset-test
  "Durable OR-Set: add/observed-remove/re-add, add-wins under concurrency,
   cross-store convergence, durability, and the idempotent content-tag variant
   (the registry shape)."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-orset :as o])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- mem [] {:backend :memory :id (random-uuid)})
(defn- tmpdir [] (str (Files/createTempDirectory "ygg-orset" (make-array FileAttribute 0))))

(deftest add-remove-readd
  (testing "remove actually removes (unlike a G-Set); re-add brings it back"
    (let [s (-> (o/durable-orset "reg" :store-config (mem)) (o/add :x) (o/add :y))]
      (is (= #{:x :y} (o/elements s)))
      (o/remove-elem s :x)
      (is (= #{:y} (o/elements s)) "observed-remove drops :x")
      (o/add s :x)
      (is (= #{:x :y} (o/elements s)) "re-add (fresh tag) brings :x back")
      (is (true? (c/-conflict-free? s)))
      (is (= :orset (p/system-type s))))))

(deftest add-wins-concurrency
  (testing "a concurrent add the remover didn't observe SURVIVES the merge"
    (let [a (-> (o/durable-orset "reg" :store-config (mem)) (o/add :k))
          b (-> (o/durable-orset "reg" :store-config (mem)) (o/add :k))]
      ;; a removes its observed :k; b's :k (different tag) is unobserved by a
      (o/remove-elem a :k)
      (is (= #{} (o/elements a)) "a removed its own :k")
      ;; converge: b's live add-tag for :k is not tombstoned ⇒ :k survives
      (o/merge-peer! a b)
      (is (= #{:k} (o/elements a)) "add-wins: b's concurrent add survives a's remove"))))

(deftest cross-peer-converges
  (testing "two peers with disjoint add/remove ops converge to the same set"
    (let [a (-> (o/durable-orset "reg" :store-config (mem)) (o/add :a1) (o/add :shared))
          b (-> (o/durable-orset "reg" :store-config (mem)) (o/add :b1) (o/add :shared))]
      (o/remove-elem b :shared)             ; b removes its own observed :shared
      (o/merge-peer! a b) (o/flush! a)
      (o/merge-peer! b a) (o/flush! b)
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
    (let [s (o/durable-orset "reg" :store-config (mem) :tag-fn identity)]
      (o/add s :x) (o/add s :x) (o/add s :x)
      (is (= #{:x} (o/elements s)))
      ;; only ONE [:x :x] pair exists (tag = element) ⇒ idempotent
      (is (= 1 (count (filter (fn [[e _]] (= e :x)) (seq @(:adds-atom s)))))
          "idempotent add: a single content-tagged pair"))))

(deftest orset-store-layout-is-crdt-walkable
  (testing "both halves live under :crdt/roots — the shape konserve-sync's crdt
            walker syncs (the walk itself is covered in konserve-sync)"
    (let [s (-> (o/durable-orset "reg" :store-config (mem))
                (o/add :a) (o/add :b) (o/remove-elem :a) o/flush!)
          roots (d/load-roots (:kv-store s))]
      (is (contains? roots :adds))
      (is (contains? roots :removals)))))
