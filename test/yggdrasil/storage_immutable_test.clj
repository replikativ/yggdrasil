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

(deftest pss-nodes-marked-immutable-roots-not
  (testing "G-Set PSS nodes are :immutable?; the mutable :crdt/roots cell is not"
    (let [a    (-> (g/gset "im" {:store-config (mem)} {:sync? true})
                   (g/conj :x) (g/conj :y) (g/flush!))
          kv   (:kv-store a)
          node (:main (d/load-roots kv {} {:sync? true}))]
      (is (some? node) "a content-addressed root node address exists")
      (is (true? (immutable? kv node)) "the PSS node is marked immutable")
      (is (nil? (immutable? kv :crdt/roots)) "the mutable roots cell is NOT marked"))))

(deftest cdvcs-graph-nodes-marked-immutable
  (testing "CDVCS commit-graph nodes (commit values inlined) are :immutable?; the
            :cdvcs/state cache cell + :cdvcs/graph roots cell are not"
    (let [a    (cd/cdvcs "doc" {:author "al" :store-config (mem)} {:sync? true})
          a    (cd/commit a "al" [[:assoc :x 1]])
          a    (p/commit! a)                              ; flush graph + state
          kv   (:kv-store a)
          node (:main (d/load-roots kv {:roots-key :cdvcs/graph} {:sync? true}))]
      (is (some? node) "a content-addressed graph root node exists")
      (is (true? (immutable? kv node)) "the graph PSS node (with inlined commit) is immutable")
      (is (nil? (immutable? kv :cdvcs/state)) "the mutable :heads/:version cache is NOT marked")
      (is (nil? (immutable? kv :cdvcs/graph)) "the mutable graph roots cell is NOT marked"))))

(deftest snapshot-blob-marked-immutable
  (testing "an addressable snapshot handle (store-commit!) is :immutable?"
    (let [a    (cd/cdvcs "snap" {:author "al" :store-config (mem)} {:sync? true})
          a    (cd/commit a "al" [[:assoc :x 1]])
          snap (p/snapshot-id a)                          ; stores a content-addressed handle
          kv   (:kv-store a)]
      (is (true? (immutable? kv (parse-uuid (str snap))))
          "the snapshot blob is marked immutable"))))
