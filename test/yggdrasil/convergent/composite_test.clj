(ns yggdrasil.convergent.composite-test
  "Spike L4-core: merging two PEER workspaces is a symmetric system-merge of
   their composites — no parent, no new interface (just -join)."
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.convergent :as c]
            [yggdrasil.composite :as comp]
            [yggdrasil.convergent.composite]
            [yggdrasil.convergent.gset :as gs]))

(defn- ws
  "A workspace composite with G-Sets {id -> initial-elems}."
  [systems]
  (comp/composite (for [[id elems] systems]
                    (reduce gs/add (gs/gset id) elems))
                  :name "ws"))

(defn- kb [composite id]
  (gs/elements (comp/get-subsystem composite id)))

(deftest test-peer-workspace-merge
  (testing "two peer workspaces (separate replicas) converge by joining their
            composites — symmetric, no parent"
    (let [a (ws {"kb" [:a1 :shared] "tags" [:x]})
          b (ws {"kb" [:b1 :shared] "tags" [:y]})]
      (testing "fan-out: every matching sub-system joins"
        (let [m (c/-join a b)]
          (is (= #{:a1 :b1 :shared} (kb m "kb")))
          (is (= #{:x :y} (kb m "tags")))))
      (testing "symmetric: join(a,b) ≡ join(b,a) per sub-system"
        (is (= (kb (c/-join a b) "kb") (kb (c/-join b a) "kb")))
        (is (= (kb (c/-join a b) "tags") (kb (c/-join b a) "tags"))))
      (testing "joining does not mutate the peers"
        (is (= #{:a1 :shared} (kb a "kb")))
        (is (= #{:b1 :shared} (kb b "kb"))))
      (testing "the whole workspace advertises conflict-free"
        (is (true? (c/-conflict-free? a)))
        (is (true? (c/convergent? a)))))))

(deftest test-three-peer-convergence
  (testing "order-independent convergence over three peer workspaces"
    (let [a (ws {"kb" [:a]}) b (ws {"kb" [:b]}) cc (ws {"kb" [:c]})]
      (is (= #{:a :b :c}
             (kb (c/join a b cc) "kb")
             (kb (c/join cc a b) "kb")
             (kb (c/join b cc a) "kb"))))))

(deftest test-system-only-on-one-peer
  (testing "a system present on only one peer survives the join"
    (let [a (ws {"kb" [:a] "only-a" [:z]})
          b (ws {"kb" [:b]})]
      (let [m (c/-join a b)]
        (is (= #{:a :b} (kb m "kb")))
        (is (= #{:z} (kb m "only-a")))))))
