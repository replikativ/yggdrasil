(ns yggdrasil.convergent.composite-test
  "Spike L4-core: merging two PEER workspaces is a symmetric system-merge of
   their composites — no parent, no new interface (just -join)."
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.composite :as comp]
            [yggdrasil.convergent.composite]
            [yggdrasil.convergent.durable-gset :as g]))

;; A minimal VERSIONED (non-convergent) system fixture: a register on a shared
;; store, Mergeable (merge! = take-theirs) but NOT PConvergent — so a composite
;; -join routes it through the versioned 3-way branch (checkout + merge!), the
;; same path datahike/git take.
(defrecord VersionedCell [id store current]
  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :cell)
  (capabilities [_] {:mergeable true :graphable false})
  p/Snapshotable
  (snapshot-id [_] (str (hash [current (get @store current)])))
  (parent-ids [_] #{})
  (as-of [_ _] (get @store current)) (as-of [_ _ _] (get @store current))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})
  p/Branchable
  (branches [_] (set (keys @store))) (branches [_ _] (set (keys @store)))
  (current-branch [_] current)
  (branch! [this name] (swap! store assoc name (get @store current)) this)
  (branch! [this name from] (swap! store assoc name (get @store from)) this)
  (branch! [this name from _] (swap! store assoc name (get @store from)) this)
  (delete-branch! [this name] (swap! store dissoc name) this)
  (delete-branch! [this name _] (swap! store dissoc name) this)
  (checkout [this name] (assoc this :current name))
  (checkout [this name _] (assoc this :current name))
  p/Mergeable
  (merge! [this source] (p/merge! this source nil))
  (merge! [this source _] (swap! store assoc current (get @store source)) this) ; take-theirs
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {}))

(defn- ws
  "A workspace composite with G-Sets {id -> initial-elems}."
  [systems]
  (comp/composite (for [[id elems] systems]
                    (reduce g/conj (g/durable-gset id) elems))
                  :name "ws"))

(defn- kb [composite id]
  (g/elements (comp/get-subsystem composite id)))

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

(deftest test-mixed-workspace-join
  (testing "a workspace with BOTH a CRDT (G-Set) and a versioned (cell) sub-system:
            -join joins the CRDT symmetrically AND 3-way-merges the versioned one
            (the versioned branch = merge-to-parent!'s per-system logic, now in
            the composite). Versioned subs share a store (resolvable ancestor)."
    (let [cell-store (atom {:a "val-a" :b "val-b"})
          ws-a (comp/composite [(reduce g/conj (g/durable-gset "kb") [:a1 :shared])
                                (->VersionedCell "cfg" cell-store :a)] :name "ws")
          ws-b (comp/composite [(reduce g/conj (g/durable-gset "kb") [:b1 :shared])
                                (->VersionedCell "cfg" cell-store :b)] :name "ws")
          merged (c/-join ws-a ws-b)]
      (testing "CRDT sub joins (union)"
        (is (= #{:a1 :b1 :shared} (kb merged "kb"))))
      (testing "versioned sub goes through merge! (take-theirs): :a took :b's value"
        (is (= "val-b" (-> (comp/get-subsystem merged "cfg") :store deref (get :a)))))
      (testing "a mixed workspace is NOT conflict-free (has a versioned sub)"
        (is (false? (c/-conflict-free? ws-a)))
        (is (false? (c/convergent? (comp/get-subsystem ws-a "cfg"))))))))

(deftest test-system-only-on-one-peer
  (testing "a system present on only one peer survives the join"
    (let [a (ws {"kb" [:a] "only-a" [:z]})
          b (ws {"kb" [:b]})]
      (let [m (c/-join a b)]
        (is (= #{:a :b} (kb m "kb")))
        (is (= #{:z} (kb m "only-a")))))))
