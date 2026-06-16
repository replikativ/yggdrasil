(ns yggdrasil.convergent.ormap-test
  "Observed-Remove Map as a conflict-free yggdrasil system."
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.ormap :as om]))

(deftest test-basic-ops
  (testing "assoc / lookup (live value-set) / keys / dissoc"
    (let [m (-> (om/ormap "kb") (om/assoc-key :a 1) (om/assoc-key :b 2))]
      (is (= #{1} (om/lookup m :a)))
      (is (= #{:a :b} (om/ormap-keys m)))
      (let [m (om/dissoc-key m :a)]                  ; value-semantic: rebind
        (is (nil? (om/lookup m :a)))
        (is (= #{:b} (om/ormap-keys m)))
        (testing "re-assoc after remove brings the key back (fresh tag)"
          (let [m (om/assoc-key m :a 9)]
            (is (= #{9} (om/lookup m :a)))))))))

(deftest test-peer-join
  (testing "two peer replicas converge — keys union, symmetric"
    (let [a (-> (om/ormap "kb") (om/assoc-key :x 1))
          b (-> (om/ormap "kb") (om/assoc-key :y 2))
          m (c/-join a b)]
      (is (= #{1} (om/lookup m :x)))
      (is (= #{2} (om/lookup m :y)))
      (is (= #{:x :y} (om/ormap-keys m)))
      (is (= (om/ormap-keys (c/-join a b)) (om/ormap-keys (c/-join b a))))
      (testing "join does not mutate peers"
        (is (= #{:x} (om/ormap-keys a)))
        (is (= #{:y} (om/ormap-keys b)))))))

(deftest test-add-wins
  (testing "a concurrent add survives a remove that didn't observe it (add-wins)"
    (let [a (-> (om/ormap "kb") (om/assoc-key :k 1)) ; a: :k=1 (tag-a)
          b (-> (om/ormap "kb") (om/assoc-key :k 2)) ; b (separate replica): :k=2 (tag-b)
          a (om/dissoc-key a :k)]                    ; a removes ONLY its own tag-a (rebind)
      (let [m (c/-join a b)]
        (is (= #{:k} (om/ormap-keys m)) "b's concurrent add keeps :k alive")
        (is (= #{2} (om/lookup m :k)) "only b's value survives; a's was tombstoned")))))

(deftest test-concurrent-values
  (testing "concurrent writes to the same key → live value-SET (multi-value)"
    (let [a (-> (om/ormap "kb") (om/assoc-key :k :from-a))
          b (-> (om/ormap "kb") (om/assoc-key :k :from-b))
          m (c/-join a b)]
      (is (= #{:from-a :from-b} (om/lookup m :k))))))

(deftest test-conflict-free-system
  (testing "valid conflict-free yggdrasil system"
    (let [m (om/ormap "kb")]
      (is (true? (c/-conflict-free? m)))
      (is (= [] (p/conflicts m "a" "b")))
      (is (= :ormap (p/system-type m))))))
