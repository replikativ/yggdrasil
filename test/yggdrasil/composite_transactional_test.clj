(ns yggdrasil.composite-transactional-test
  "A composite merge is transactional by default: `commit!` flushes every
   sub-system durable, then writes `:composite/root` LAST. So sub-state becomes
   durable atomically with the composite snapshot — a reader (resolving through
   `:composite/root`) never sees a half-merge, and a crash before the root write
   leaves the previous committed composite as the latest."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [yggdrasil.protocols :as p]
            [yggdrasil.composite :as cmp]
            [yggdrasil.convergent.durable-gset :as g])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- file-cfg [] {:backend :file :id (random-uuid)
                    :path (str (Files/createTempDirectory "ygg-comp" (make-array FileAttribute 0)))})

(deftest commit-flushes-all-subs-then-root
  (testing "sub-systems are durable only after the composite commits"
    (let [sc-a (file-cfg) sc-b (file-cfg) sc-comp (file-cfg)
          a (-> (g/durable-gset "a" :store-config sc-a) (g/add :a1) (g/add :a2))
          b (-> (g/durable-gset "b" :store-config sc-b) (g/add :b1))
          comp (cmp/composite [a b] :store-config sc-comp)]
      ;; pre-commit: the sub edits are in-memory only — a fresh reopen sees nothing
      (is (empty? (g/elements (g/durable-gset "a" :store-config sc-a))))
      (is (empty? (g/elements (g/durable-gset "b" :store-config sc-b))))
      ;; commit the composite → flushes every sub, then writes :composite/root
      (let [committed (p/commit! comp "snapshot-1")]
        (is (some? (p/snapshot-id committed)))
        ;; post-commit: every sub is durable (reopen each store → merged state)
        (is (= #{:a1 :a2} (g/elements (g/durable-gset "a" :store-config sc-a))))
        (is (= #{:b1} (g/elements (g/durable-gset "b" :store-config sc-b))))
        ;; the composite root names a bundle referencing both sub snapshots
        (let [root (k/get (:kv-store committed) :composite/root nil {:sync? true})]
          (is (some? root) ":composite/root advanced"))
        (let [meta (p/snapshot-meta committed (p/snapshot-id committed))]
          (is (= #{"a" "b"} (set (keys (:sub-snapshots meta))))
              "the committed bundle references every sub-system"))))))

(deftest merge-commits-transactionally
  (testing "composite merge! delegates to the transactional commit"
    (let [a (-> (g/durable-gset "a" :store-config (file-cfg)) (g/add :x))
          b (-> (g/durable-gset "b" :store-config (file-cfg)) (g/add :y))
          comp (cmp/composite [a b] :store-config (file-cfg))
          ;; merging :main into :main is a no-op union, but it must still commit
          merged (p/merge! comp :main {})]
      (is (some? (p/snapshot-id merged)))
      (is (some? (p/snapshot-meta merged (p/snapshot-id merged)))
          "merge produced a committed composite snapshot"))))
