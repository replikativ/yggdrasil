(ns yggdrasil.convergent.ormap-test
  "Durable OR-Map (flattened [k uid v] over the durable-set substrate) + its
   merging variant: multi-value reads vs merge-fn fold, observed-remove/add-wins,
   cross-store convergence, durable reopen, snapshot/as-of, δ ≡ -join. PORTABLE —
   one .cljc body per concern (JVM sync / cljs async via `<?`)."
  (:require #?(:clj  [clojure.test :refer [is testing]]
               :cljs [cljs.test :refer [is testing]])
            [yggdrasil.test-async :refer [deftest-async <? sync? file-cfg]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.ormap :as om])
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [is.simm.partial-cps.async :refer [async]])))

(deftest-async plain-assoc-get-dissoc-multivalue
  (testing "plain OR-Map: get returns the live value-SET; re-assoc accumulates"
    (let [m (<? (om/ormap "m" :store-config (file-cfg) :sync? sync?))
          m (<? (om/assoc m :a 1)) m (<? (om/assoc m :a 2)) m (<? (om/assoc m :b 9))]
      (is (= #{1 2} (<? (om/get m :a))) "concurrent/repeated writes → value-set")
      (is (= #{9} (<? (om/get m :b))))
      (is (= #{:a :b} (<? (om/keys m))))
      (is (= :ormap (p/system-type m)))
      (let [m (<? (om/dissoc m :a))]
        (is (nil? (<? (om/get m :a))) "observed-remove drops :a")
        (is (= #{9} (<? (om/get m :b))) "untouched")
        (is (= #{:b} (<? (om/keys m))))))))

(deftest-async add-wins-concurrency
  (testing "a concurrent add the remover didn't observe SURVIVES the merge"
    (let [a (<? (om/ormap "m" :store-config (file-cfg) :sync? sync?))
          a (<? (om/assoc a :k 1))
          b (<? (om/ormap "m" :store-config (file-cfg) :sync? sync?))
          b (<? (om/assoc b :k 2))
          a (<? (om/dissoc a :k))]
      (is (nil? (<? (om/get a :k))) "a removed its own tag")
      (let [a (<? (om/merge-peer! a b))]
        (is (= #{2} (<? (om/get a :k))) "add-wins: b's concurrent add survives")))))

(deftest-async merging-variant-folds
  (testing "merging OR-Map: get FOLDS live values via merge-fn (here max)"
    (let [m (<? (om/merging-ormap "m" max :store-config (file-cfg) :sync? sync?))
          m (<? (om/assoc m :a 3)) m (<? (om/assoc m :a 7)) m (<? (om/assoc m :a 5))]
      (is (= 7 (<? (om/get m :a))) "single folded value, not a set")
      (is (= :merging-ormap (p/system-type m))))))

(deftest-async cross-store-merging-converges
  (testing "two stores, merge-fn fold: peers converge to the same folded value"
    (let [a (<? (om/merging-ormap "m" max :store-config (file-cfg) :sync? sync?))
          a (<? (om/assoc a :score 5))
          b (<? (om/merging-ormap "m" max :store-config (file-cfg) :sync? sync?))
          b (<? (om/assoc b :score 9))
          a (<? (om/merge-peer! a b))
          b (<? (om/merge-peer! b a))]
      (is (= 9 (<? (om/get a :score))) "max(5,9)")
      (is (= (<? (om/get a :score)) (<? (om/get b :score))) "strong eventual consistency"))))

(deftest-async delta-op-perspective
  (testing "δ: assoc/dissoc record {:adds}/{:removals} triples; apply-delta ≡ -join"
    (let [m (<? (om/ormap "a" :store-config (file-cfg) :sync? sync?))
          m (<? (om/assoc m :x 1)) m (<? (om/assoc m :y 2))
          dl (c/delta-of m)
          peer  (<? (om/ormap "b" :store-config (file-cfg) :sync? sync?))
          peer  (<? (om/assoc peer :z 3))]
      (is (= #{:x :y} (set (map #(nth % 2) (:adds dl)))) "δ :adds carries the [hk uid k v] entries")
      (let [via-op (<? (c/-apply-delta peer dl))]
        (is (= #{:x :y :z} (<? (om/keys via-op))) "ops integrated")))))

(deftest-async snapshot-as-of-freeze
  (testing "content-addressed snapshot-id + as-of restores the frozen map value"
    (let [m   (<? (om/ormap "m" :store-config (file-cfg) :sync? sync?))
          m   (<? (om/assoc m :a 1)) m (<? (om/assoc m :b 2))
          sid (<? (p/snapshot-id m))
          m   (<? (om/dissoc m :a)) m (<? (om/assoc m :c 3))]
      (is (= #{:b :c} (<? (om/keys m))))
      (is (= {:a #{1} :b #{2}} (<? (p/as-of m sid))) "as-of restores the frozen value-set map"))))

;; portable file-backed durability
(deftest-async durable-across-reopen
  (testing "flush, reopen, restore the flattened halves"
    (let [sc (file-cfg)
          m  (<? (om/ormap "m" :store-config sc :sync? sync?))
          m  (<? (om/assoc m :p 1)) m (<? (om/assoc m :q 2))
          m  (<? (om/dissoc m :q))
          _  (<? (om/flush! m))
          re (<? (om/ormap "m" :store-config sc :sync? sync?))]
      (is (= #{:p} (<? (om/keys re))) "removal persisted across reopen")
      (is (= #{1} (<? (om/get re :p)))))))
