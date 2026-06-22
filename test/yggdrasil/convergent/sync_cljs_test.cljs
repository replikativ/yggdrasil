(ns yggdrasil.convergent.sync-cljs-test
  "ClojureScript SYNC mode (`:sync? true`) over the sync-capable konserve backends
   (memory + node filestore). The PSS catalog reads-on-the-fly even synchronously on
   cljs: `pss/seq`/`pss/slice` return a plain `Iter` (an `ISeq`) under `:sync? true`,
   drained by `clojure.core/into`, so `elements`/`get`/`-join` work WITHOUT the async
   `AsyncSeq` path — values flow directly, like the JVM. (Regression guard: draining
   the sync `Iter` through the partial-cps AsyncSeq protocol previously ran away →
   heap OOM.) Browser/IndexedDB has no synchronous konserve API, so this is node-only."
  (:require [cljs.test :refer [deftest is]]
            [yggdrasil.convergent.gset :as g]
            [yggdrasil.convergent.orset :as o]
            [yggdrasil.convergent.twopset :as t]
            [yggdrasil.convergent.ormap :as om]))

(defn- mem [] {:backend :memory :id (random-uuid)})
(defn- file [] {:backend :file :id (random-uuid)
                :path (str (.tmpdir (js/require "os")) "/ygg-sync-" (random-uuid))})

(deftest gset-elements-sync-memory          ; set->clj drain over the sync Iter
  (let [s (-> (g/gset "t" {:store-config (mem)} {:sync? true}) (g/conj :a) (g/conj :b))]
    (is (= #{:a :b} (g/elements s)))
    (is (true? (g/contains? s :a)))))

(deftest gset-elements-sync-node-filestore  ; same drain, real node fs + flush/restore
  (let [s (g/flush! (-> (g/gset "t" {:store-config (file)} {:sync? true})
                        (g/conj :x) (g/conj :y)))]
    (is (= #{:x :y} (g/elements s)))))

(deftest set-union-sync                       ; merge-peer! exercises set-union's sync reduce
  (let [a (-> (g/gset "a" {:store-config (mem)} {:sync? true}) (g/conj :a) (g/conj :shared))
        b (-> (g/gset "b" {:store-config (mem)} {:sync? true}) (g/conj :b) (g/conj :shared))]
    (is (= #{:a :b :shared} (g/elements (g/merge-peer! a b))))))

(deftest ormap-get-sync                       ; slice->clj range read over the sync Iter
  (let [m (-> (om/ormap "kb" {:store-config (mem)} {:sync? true}) (om/assoc :k 1) (om/assoc :k 2))]
    (is (= #{1 2} (om/get m :k)))))

(deftest orset-twopset-sync                   ; set->clj over both halves + difference
  (let [s (-> (o/orset "r" {:store-config (mem)} {:sync? true}) (o/conj :a) (o/conj :b) (o/disj :a))
        tp (-> (t/twopset "p" {:store-config (mem)} {:sync? true}) (t/conj 1) (t/conj 2) (t/disj 1))]
    (is (= #{:b} (o/elements s)))
    (is (= #{2} (t/elements tp)))))
