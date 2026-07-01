(ns yggdrasil.storage-immutable-test
  "Step 2 — content-addressed, write-once values carry konserve's `:immutable?`
   metadata so a bidirectional sync peer can skip re-storing (and re-publishing) a
   value it already holds; MUTABLE cells (roots / heads pointers) stay UNMARKED —
   they ride the convergent δ path, not the node push.

   JVM-only (sync): the marking is the SAME `kb/immutable-meta` arg in both the clj
   and cljs `KonserveStorage.store` branches, so a synchronous JVM check is
   sufficient. (`konserve.core/get-meta` returns a core.async channel under
   `:sync? false`, which the partial-cps `<?` can't await — hence not portable.)"
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.gset :as g]
            [yggdrasil.convergent.cdvcs :as cd]))

(defn- mem [] {:backend :memory :id (random-uuid)})

(defn- immutable? [kv key]
  (:immutable? (k/get-meta kv key nil {:sync? true})))

(deftest pss-nodes-marked-immutable-heads-not
  (testing "G-Set PSS CHILD nodes are :immutable?; the mutable head/registry cells are not
            (the fused root is inlined in the head cell, not a store object — a multi-node
            tree is needed for a stored, markable node)"
    (let [a     (g/flush! (reduce g/conj (g/gset "im" {:store-config (mem)} {:sync? true}) (range 300)))
          kv    (:kv-store a)
          root  (:root (d/load-head kv :main {} {:sync? true}))
          child (first (d/reachable-of kv root {:sync? true}))]
      (is (some? child) "a stored content-addressed child node exists (multi-node tree)")
      (is (true? (immutable? kv child)) "the PSS node is marked immutable")
      (is (nil? (immutable? kv :crdt.head/main)) "the mutable head cell is NOT marked")
      (is (nil? (immutable? kv :crdt/branches)) "the mutable registry is NOT marked"))))

(deftest cdvcs-graph-nodes-marked-immutable
  (testing "CDVCS commit-graph CHILD nodes (commit values inlined) are :immutable?; the
            :cdvcs/state cache cell + the mutable head cell are not (many commits ⇒ a
            multi-node graph with stored, markable children)"
    (let [a    (reduce (fn [a i] (p/commit! (cd/commit a "al" [[:assoc :k i]])))
                       (cd/cdvcs "doc" {:author "al" :store-config (mem)} {:sync? true})
                       (range 100))
          kv   (:kv-store a)
          root (:root (d/load-head kv :main {:cell-ns "cdvcs"} {:sync? true}))
          child (first (d/reachable-of kv root {:sync? true}))]
      (is (some? child) "a stored graph child node exists (multi-node)")
      (is (true? (immutable? kv child)) "the graph PSS node (with inlined commit) is immutable")
      (is (nil? (immutable? kv :cdvcs/state)) "the mutable :heads/:version cache is NOT marked")
      (is (nil? (immutable? kv (keyword "crdt.head" "cdvcs::main"))) "the mutable head cell is NOT marked"))))

(deftest snapshot-blob-marked-immutable
  (testing "an addressable snapshot handle (store-commit!) is :immutable?"
    (let [a    (cd/cdvcs "snap" {:author "al" :store-config (mem)} {:sync? true})
          a    (cd/commit a "al" [[:assoc :x 1]])
          snap (p/snapshot-id a)                          ; stores a content-addressed handle
          kv   (:kv-store a)]
      (is (true? (immutable? kv (parse-uuid (str snap))))
          "the snapshot blob is marked immutable"))))
