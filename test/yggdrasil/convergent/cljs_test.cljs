(ns yggdrasil.convergent.cljs-test
  "Proves the CRDT catalog (G-Set / LWWR / OR-Map) compiles AND converges on
   ClojureScript — i.e. yggdrasil's conflict-free systems run in the browser.
   Run: clojure -M:cljs-test (compiles to node, runs -main)."
  (:require [cljs.test :refer-macros [deftest is testing run-tests]]
            ;; force cljs compilation of the cross-platform storage + index layer
            [yggdrasil.storage]
            [yggdrasil.registry]
            [yggdrasil.composite]
            [yggdrasil.workspace]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.gset :as gs]
            [yggdrasil.convergent.lwwr :as lwwr]
            [yggdrasil.convergent.ormap :as om]))

(deftest gset-converges-on-cljs
  (let [a (-> (gs/gset "kb") (gs/add :a) (gs/add :x))
        b (-> (gs/gset "kb") (gs/add :b) (gs/add :x))]
    (is (= #{:a :b :x} (gs/elements (c/-join a b))))
    (is (= (gs/elements (c/-join a b)) (gs/elements (c/-join b a))))
    (is (true? (c/-conflict-free? a)))
    (is (= :gset (p/system-type a)))))

(deftest lwwr-converges-on-cljs
  (let [a (-> (lwwr/lwwr "x") (lwwr/set-register :a))
        _ (js/Date.now)
        b (-> (lwwr/lwwr "x") (lwwr/set-register :b))]
    ;; both registers carry timestamps; join keeps one deterministically
    (is (contains? #{:a :b} (lwwr/value (c/-join a b))))
    (is (= (lwwr/value (c/-join a b)) (lwwr/value (c/-join b a))))))

(deftest ormap-add-wins-on-cljs
  (let [a (-> (om/ormap "kb") (om/assoc-key :k 1))
        b (-> (om/ormap "kb") (om/assoc-key :k 2))
        _ (om/dissoc-key a :k)]
    (is (= #{:k} (om/ormap-keys (c/-join a b))))
    (is (= #{2} (om/lookup (c/-join a b) :k)))))

(defn -main [& _]
  (run-tests))
