(ns yggdrasil.convergent.gset-test
  "Spike: prove a G-Set is a conflict-free yggdrasil system whose merge is a
   SYMMETRIC peer join (no parent, no ancestor, no conflicts)."
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.gset :as gs]))

(deftest test-local-ops
  (testing "add accumulates into the current branch's set"
    (let [g (-> (gs/gset "kb") (gs/add :a) (gs/add :b) (gs/add :a))]
      (is (= #{:a :b} (gs/elements g))))))

(deftest test-symmetric-join
  (testing "join of two PEER replicas is commutative, idempotent, associative —
            no parent, no ancestor"
    (let [a (-> (gs/gset "kb") (gs/add :a) (gs/add :x))
          b (-> (gs/gset "kb") (gs/add :b) (gs/add :x))
          cc (-> (gs/gset "kb") (gs/add :c))]
      (testing "commutative: a∪b = b∪a"
        (is (= #{:a :b :x} (gs/elements (c/-join a b))))
        (is (= (gs/elements (c/-join a b)) (gs/elements (c/-join b a)))))
      (testing "idempotent: a∪a = a"
        (is (= (gs/elements a) (gs/elements (c/-join a a)))))
      (testing "associative: (a∪b)∪c = a∪(b∪c)"
        (is (= (gs/elements (c/-join (c/-join a b) cc))
               (gs/elements (c/-join a (c/-join b cc))))))
      (testing "join does not mutate the peers (pure)"
        (is (= #{:a :x} (gs/elements a)))
        (is (= #{:b :x} (gs/elements b))))
      (testing "order-independent convergence over many peers"
        (is (= (gs/elements (c/join a b cc))
               (gs/elements (c/join cc a b))
               (gs/elements (c/join b cc a))))))))

(deftest test-conflict-free-capability
  (testing "advertises conflict-free; never reports conflicts"
    (let [g (gs/gset "kb")]
      (is (true? (c/-conflict-free? g)))
      (is (true? (c/convergent? g)))
      (is (= [] (p/conflicts g "x" "y"))))))

(deftest test-is-a-yggdrasil-system
  (testing "satisfies the yggdrasil system protocols + branch/checkout/merge work"
    (let [g (gs/gset "kb" :init #{:seed})]
      (is (satisfies? p/SystemIdentity g))
      (is (satisfies? p/Snapshotable g))
      (is (satisfies? p/Branchable g))
      (is (satisfies? p/Mergeable g))
      (is (= :gset (p/system-type g)))
      (is (= :main (p/current-branch g)))
      (testing "branch! copies the branch value; fork writes are isolated (value-semantic)"
        (let [forked (-> g (p/branch! :fork) (p/checkout :fork) (gs/add :only-fork))]
          (is (= #{:seed :only-fork} (gs/elements forked)))
          ;; the original value on :main does NOT see the fork's write
          (is (= #{:seed} (gs/elements (p/checkout forked :main))))
          (testing "branch-merge IS the join (conflict-free), through Mergeable"
            (let [merged (-> forked (p/checkout :main) (p/merge! :fork))]
              (is (= #{:seed :only-fork} (gs/elements merged))))))))
    (testing "snapshot-id is content-stable (same content ⇒ same id, regardless
              of system-id) — two fresh, unentangled replicas"
      (is (= (p/snapshot-id (gs/gset "kb" :init #{:seed}))
             (p/snapshot-id (gs/gset "other" :init #{:seed}))))
      (is (not= (p/snapshot-id (gs/gset "kb" :init #{:seed}))
                (p/snapshot-id (gs/gset "kb" :init #{:other})))))))

(deftest delta-op-perspective
  (testing "in-mem catalog δ (via the generic system): ops recorded at the write
            (no diffing); apply-delta ≡ -join; a no-op -join returns identical
            (signal-safe, like the durable runaway guard)"
    (let [g (-> (gs/gset "a") (gs/add :x) (gs/add :y))]
      (is (= #{:x :y} (c/delta-of g)) "δ = the ops (accrued by vjoin), no diffing")
      (is (nil? (c/delta-of (c/clear-delta g))) "clear-delta drops it")
      ;; op-path: a peer applies just the δ; ≡ the state-path (-join)
      (let [peer (gs/gset "c" :init #{:z})]
        (is (= #{:x :y :z} (gs/elements (c/-apply-delta peer (c/delta-of g)))) "apply-delta unions ops in")
        (is (= (gs/elements (c/-apply-delta (gs/gset "c" :init #{:z}) (c/delta-of g)))
               (gs/elements (c/-join (gs/gset "c" :init #{:z}) g)))
            "op-path ≡ state-path"))
      ;; issue-2 idempotence for the in-mem catalog too
      (is (identical? g (c/-join g (gs/gset "d" :init #{:x}))) "no-op -join returns the receiver identical"))))
