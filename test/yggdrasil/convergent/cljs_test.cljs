(ns yggdrasil.convergent.cljs-test
  "cljs compile-smoke + the in-memory LWWR convergence on ClojureScript. The
   collection CRDTs (G-Set / OR-Map / 2P-Set / OR-Set) are now durable-only (PSS
   over konserve, async on cljs) and proven on node by their own durable_*_test
   ns; this file just (a) forces cljs compilation of the storage/registry/
   composite/workspace layer and (b) covers LWWR, the one in-memory (sync) CRDT."
  (:require [cljs.test :refer-macros [deftest is testing run-tests]]
            ;; force cljs compilation of the cross-platform storage + index layer
            [yggdrasil.storage]
            [yggdrasil.registry]
            [yggdrasil.composite]
            [yggdrasil.workspace]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.lwwr :as lwwr]))

(deftest lwwr-converges-on-cljs
  (let [a (-> (lwwr/lwwr "x") (lwwr/set-register :a))
        _ (js/Date.now)
        b (-> (lwwr/lwwr "x") (lwwr/set-register :b))]
    ;; both registers carry timestamps; join keeps one deterministically
    (is (contains? #{:a :b} (lwwr/value (c/-join a b))))
    (is (= (lwwr/value (c/-join a b)) (lwwr/value (c/-join b a))))
    (is (true? (c/-conflict-free? a)))))

(defn -main [& _]
  (run-tests))
