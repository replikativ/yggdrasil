(ns yggdrasil.convergent.lwwr-test
  "LWW-Register as a conflict-free yggdrasil system on the generic machinery."
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.system :as sys]
            [yggdrasil.convergent.lwwr :as lwwr]))

(defn- at [id v ts]
  (sys/put! (lwwr/lwwr id) {:register v :timestamp ts}))

(deftest test-local-write
  (testing "set-register / value"
    (let [l (-> (lwwr/lwwr "x") (lwwr/set-register :hello))]
      (is (= :hello (lwwr/value l)))
      (let [l (lwwr/set-register l :world)]          ; value-semantic: rebind the new handle
        (is (= :world (lwwr/value l)))))))            ; last write wins

(deftest test-lwwr-join
  (testing "later timestamp wins; symmetric; idempotent"
    (let [a (at "x" :a 100)
          b (at "x" :b 200)]
      (is (= :b (lwwr/value (c/-join a b))))
      (is (= :b (lwwr/value (c/-join b a))))      ; symmetric
      (is (= :a (lwwr/value (c/-join a a))))))    ; idempotent
  (testing "tie on timestamp → deterministic winner on every replica"
    (let [a (at "x" :a 100)
          b (at "x" :b 100)]
      (is (= (lwwr/value (c/-join a b)) (lwwr/value (c/-join b a)))))))

(deftest test-conflict-free-system
  (testing "advertises conflict-free + is a valid yggdrasil system"
    (let [l (lwwr/lwwr "x" :init :seed)]
      (is (true? (c/-conflict-free? l)))
      (is (= [] (p/conflicts l "a" "b")))
      (is (= :lwwr (p/system-type l)))
      (is (= :seed (lwwr/value l)))
      (testing "branch-merge is the join (take-later)"
        (let [l2 (-> l (p/branch! :fork) (p/checkout :fork))
              _ (Thread/sleep 2)
              l2 (lwwr/set-register l2 :forked)]
          (is (= :forked (lwwr/value (-> l2 (p/checkout :main) (p/merge! :fork))))))))))
