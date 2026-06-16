(ns yggdrasil.convergent.lwwr-test
  "LWW-Register as a conflict-free yggdrasil system on the generic machinery."
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.system :as sys]
            [yggdrasil.convergent.lwwr :as lwwr]))

(defn- at [id v ts]
  (sys/put! (lwwr/lwwr id) {:register v :hlc [ts 0]}))

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
        ;; no sleep needed: HLC's logical counter makes the forked write strictly
        ;; later than the seed even within the same wall-clock millisecond.
        (let [l2 (-> l (p/branch! :fork) (p/checkout :fork))
              l2 (lwwr/set-register l2 :forked)]
          (is (= :forked (lwwr/value (-> l2 (p/checkout :main) (p/merge! :fork))))))))))

(deftest hlc-monotonic-under-clock-regression
  (testing "each write's HLC is strictly greater than the last, even if the
            wall-clock steps BACKWARD — so the latest write wins (no lost update)
            and a stale write at an old wall-clock can't clobber it"
    (let [clock (atom 100)]
      (binding [t/*now-fn* (fn [] @clock)]
        (let [l (-> (lwwr/lwwr "x") (lwwr/set-register :first))]   ; hlc [100 0]
          (reset! clock 90)                                         ; clock STEPS BACK
          (let [l (lwwr/set-register l :second)]                    ; hlc [100 1] (logical bumps)
            (reset! clock 95)
            (let [l (lwwr/set-register l :third)]                   ; hlc [100 2]
              (is (= :third (lwwr/value l)) "latest write wins despite clock regressions")
              (is (= 100 (lwwr/timestamp l)) "physical stays the max wall-clock seen")
              ;; a stale replica's write stamped at the OLD wall-clock loses
              (let [stale (sys/put! (lwwr/lwwr "x") {:register :stale :hlc [100 0]})]
                (is (= :third (lwwr/value (c/-join l stale)))
                    "monotonic HLC: the stale [100 0] write can't clobber [100 2]")))))))))

(deftest hlc-causal-across-peers
  (testing "a write that OBSERVED a peer (via join) ticks past it and wins even if
            its wall-clock is BEHIND — while a genuinely-concurrent write resolves
            by wall-clock (honest LWW)"
    (let [a (binding [t/*now-fn* (constantly 200)]                  ; A on a FAST clock
              (-> (lwwr/lwwr "r") (lwwr/set-register :a)))]         ; A: hlc [200 0]
      (binding [t/*now-fn* (constantly 100)]                        ; B on a SLOW clock
        ;; B OBSERVES A (joins it), adopting [200 0], then writes → ticks to [200 1]
        (let [b (-> (lwwr/lwwr "r") (c/-join a) (lwwr/set-register :b))]
          (is (= :b (lwwr/value (c/-join a b))) "causal: B observed A → B wins despite a slower clock")
          (is (= :b (lwwr/value (c/-join b a))) "symmetric")
          ;; a CONCURRENT B (never saw A) at the slow clock loses to A's higher wall-clock
          (let [b-conc (-> (lwwr/lwwr "r") (lwwr/set-register :b))] ; [100 0], didn't observe A
            (is (= :a (lwwr/value (c/-join a b-conc)))
                "concurrent: the higher wall-clock wins (honest LWW)")))))))
