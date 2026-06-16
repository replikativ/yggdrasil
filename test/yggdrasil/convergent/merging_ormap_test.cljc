(ns yggdrasil.convergent.merging-ormap-test
  "Merging-OR-Map: concurrent per-key values FOLD to one value via a commutative/
   associative/idempotent merge-fn (vs the plain OR-Map's value-set). Local fold,
   cross-replica convergence to a single merged value, dissoc, and order-
   independence. Portable plain deftest on BOTH platforms (pure, in-memory)."
  (:require #?(:clj  [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.merging-ormap :as mo]))

(deftest local-fold-via-merge-fn
  (testing "repeated assoc to a key folds into ONE value via merge-fn (here: max)"
    (let [m (-> (mo/merging-ormap "scores" max)
                (mo/assoc :a 3) (mo/assoc :a 7) (mo/assoc :a 5))]
      (is (= 7 (mo/get m :a)) "the folded value, not a set")
      (is (= #{:a} (mo/keys m))))))

(deftest concurrent-replicas-converge-to-one-merged-value
  (testing "two replicas assoc the SAME key concurrently → join folds both to a
            single merged value (NOT a conflict set); join is commutative"
    (let [a (-> (mo/merging-ormap "s" max) (mo/assoc :score 5))
          b (-> (mo/merging-ormap "s" max) (mo/assoc :score 9))
          ab (c/-join a b)
          ba (c/-join b a)]
      (is (true? (c/-conflict-free? ab)) "Merging-OR-Map auto-resolves — conflict-free")
      (is (= 9 (mo/get ab :score)) "max(5,9) — a single folded value")
      (is (= (mo/get ab :score) (mo/get ba :score)) "commutative: a∨b = b∨a")
      (testing "idempotent: re-joining changes nothing"
        (is (= ab (c/-join ab b)))))))

(deftest lww-by-timestamp-is-order-independent
  (testing "an LWW-by-:ts merge-fn resolves concurrent entity writes deterministically"
    (let [latest (fn [x y] (if (>= (:ts x) (:ts y)) x y))
          john   {:user "john"  :ts 1}
          petra  {:user "petra" :ts 2}
          fwd    (-> (mo/merging-ormap "e" latest) (mo/assoc 12 john) (mo/assoc 12 petra))
          rev    (-> (mo/merging-ormap "e" latest) (mo/assoc 12 petra) (mo/assoc 12 john))]
      (is (= petra (mo/get fwd 12)) "newest (petra) wins")
      (is (= (mo/get fwd 12) (mo/get rev 12)) "order-independent fold"))))

(deftest dissoc-removes
  (testing "dissoc tombstones the live tags; get returns nil"
    (let [m (-> (mo/merging-ormap "s" max) (mo/assoc :a 1) (mo/assoc :b 2))]
      (is (= 1 (mo/get m :a)))
      (let [m (mo/dissoc m :a)]
        (is (nil? (mo/get m :a)) "removed")
        (is (= 2 (mo/get m :b)) "untouched")
        (is (= #{:b} (mo/keys m)))))))

(deftest readd-after-remove-merges-with-concurrent-survivor
  (testing "add-wins under concurrency: a removes, b's concurrent add survives the
            join and folds (the OR-Map property, with merge-fn folding the value)"
    (let [a (-> (mo/merging-ormap "s" max) (mo/assoc :k 4))
          b (-> (mo/merging-ormap "s" max) (mo/assoc :k 9))   ; concurrent add b didn't observe
          a (mo/dissoc a :k)]                                  ; a removes its OWN tag only
      (is (nil? (mo/get a :k)) "a removed its tag")
      (let [joined (c/-join a b)]
        (is (= 9 (mo/get joined :k)) "b's concurrent add survives (add-wins) and folds")))))
