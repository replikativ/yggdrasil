(ns yggdrasil.registry-system-test
  "The registry is itself a first-class conflict-free yggdrasil system (a 2P-Set
   under the query lens). It has a content-addressed snapshot identity, converges
   by -join, and — the closure property — can be TRACKED inside another registry."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.types :as t]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable-2pset :as d2p]
            [yggdrasil.registry :as reg]))

(defn- entry [snap sys branch phys]
  (t/->RegistryEntry snap sys branch (t/->HLC phys 0) nil nil nil))

(deftest registry-is-a-conflict-free-system
  (testing "registry-system exposes the durable 2P-Set as a system"
    (let [r (-> (reg/create-registry) (reg/register! (entry "s1" "git:a" "main" 100)))
          sys (reg/registry-system r)]
      (is (= :2p-set (p/system-type sys)))
      (is (= "registry" (p/system-id sys)))
      (is (true? (c/-conflict-free? sys)))
      (is (string? (p/snapshot-id sys)) "content-addressed snapshot identity")
      (testing "snapshot-id is content-addressed: changes with content, stable for equal content"
        (let [id1 (p/snapshot-id sys)
              _ (reg/register! r (entry "s2" "git:a" "main" 200))
              ;; registry-system returns a SNAPSHOT value — re-fetch after the
              ;; mutation to observe the new content (value-semantic registry conn).
              id2 (p/snapshot-id (reg/registry-system r))
              r' (-> (reg/create-registry)
                     (reg/register! (entry "s1" "git:a" "main" 100))
                     (reg/register! (entry "s2" "git:a" "main" 200)))]
          (is (not= id1 id2) "id changes when an entry is added")
          (is (= id2 (p/snapshot-id (reg/registry-system r')))
              "two registries with equal content → equal snapshot id")))
      (reg/close! r))))

(deftest registry-systems-converge-by-join
  (testing "two registries reconcile by -join (2P-Set union of entries)"
    (let [r1 (-> (reg/create-registry) (reg/register! (entry "s1" "git:a" "main" 100)))
          r2 (-> (reg/create-registry) (reg/register! (entry "s2" "git:b" "main" 200)))
          joined (c/-join (reg/registry-system r1) (reg/registry-system r2))]
      (is (= #{"s1" "s2"} (set (map :snapshot-id (d2p/elements joined))))
          "the join is the union of both registries' entries"))))

(deftest registry-in-registry
  (testing "a registry can be tracked as a system INSIDE another registry"
    (let [;; an inner registry with some snapshots
          inner (-> (reg/create-registry)
                    (reg/register! (entry "s1" "git:a" "main" 100))
                    (reg/register! (entry "s2" "git:a" "main" 200)))
          inner-sys (reg/registry-system inner)
          ;; a meta-registry that pins the inner registry's current snapshot
          meta (-> (reg/create-registry)
                   (reg/register! (t/->RegistryEntry
                                   (p/snapshot-id inner-sys)   ; the inner registry's Merkle id
                                   (p/system-id inner-sys)     ; "registry"
                                   "main" (t/->HLC 1000 0) nil nil nil)))]
      ;; the meta-registry now indexes the inner registry as one of its systems
      (let [world (reg/as-of meta (t/->HLC 9999 0))]
        (is (contains? world ["registry" "main"]) "meta tracks the inner registry")
        (is (= (p/snapshot-id inner-sys)
               (:snapshot-id (get world ["registry" "main"])))
            "and pins its content-addressed snapshot id"))
      ;; advancing the inner registry changes its id → a NEW meta entry would pin it
      (reg/register! inner (entry "s3" "git:a" "main" 300))
      ;; re-fetch the inner system (value-semantic: registry-system is a snapshot)
      (is (not= (p/snapshot-id (reg/registry-system inner))
                (:snapshot-id (get (reg/as-of meta (t/->HLC 9999 0)) ["registry" "main"])))
          "the pinned id is the old snapshot; the inner registry has moved on")
      (reg/close! inner) (reg/close! meta))))
