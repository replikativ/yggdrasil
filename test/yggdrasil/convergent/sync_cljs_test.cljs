(ns yggdrasil.convergent.sync-cljs-test
  "ClojureScript SYNC mode (`:sync? true`) over the sync-capable konserve backends
   (memory + node filestore). The PSS catalog reads-on-the-fly even synchronously on
   cljs: `pss/seq`/`pss/slice` return a plain `Iter` (an `ISeq`) under `:sync? true`,
   drained by `clojure.core/into`, so `elements`/`get`/`-join` work WITHOUT the async
   `AsyncSeq` path — values flow directly, like the JVM. (Regression guard: draining
   the sync `Iter` through the partial-cps AsyncSeq protocol previously ran away →
   heap OOM.) Browser/IndexedDB has no synchronous konserve API, so this is node-only."
  (:require [cljs.test :refer [deftest is]]
            ;; registers konserve's `:file` backend defmethods on node, so the
            ;; node-filestore test below can open `{:backend :file …}`.
            [konserve.node-filestore]
            [yggdrasil.convergent.gset :as g]
            [yggdrasil.convergent.orset :as o]
            [yggdrasil.convergent.twopset :as t]
            [yggdrasil.convergent.ormap :as om]))

(defn- mem [] {:backend :memory :id (random-uuid)})
(defn- file [] {:backend :file :id (random-uuid)
                :path (str (.tmpdir (js/require "os")) "/ygg-sync-" (random-uuid))})

;; The record carries NO stamped sync-mode (per-call opts since the domain-config /
;; runtime-opts split) — so EVERY op needs `{:sync? true}` to run synchronously on
;; cljs, not just the constructor. Without it an op defaults to `c/default-opts`
;; (async on cljs) and returns the un-run CPS fn instead of a value.
(def ^:private so {:sync? true})

(deftest gset-elements-sync-memory          ; set->clj drain over the sync Iter
  (let [s (-> (g/gset "t" {:store-config (mem)} so) (g/conj :a so) (g/conj :b so))]
    (is (= #{:a :b} (g/elements s so)))
    (is (true? (g/contains? s :a so)))))

(deftest gset-elements-sync-node-filestore  ; same drain, real node fs + flush/restore
  (let [s (g/flush! (-> (g/gset "t" {:store-config (file)} so)
                        (g/conj :x so) (g/conj :y so))
                    so)]
    (is (= #{:x :y} (g/elements s so)))))

(deftest set-union-sync                       ; merge-peer! exercises set-union's sync reduce
  (let [a (-> (g/gset "a" {:store-config (mem)} so) (g/conj :a so) (g/conj :shared so))
        b (-> (g/gset "b" {:store-config (mem)} so) (g/conj :b so) (g/conj :shared so))]
    (is (= #{:a :b :shared} (g/elements (g/merge-peer! a b so) so)))))

(deftest ormap-get-sync                       ; slice->clj range read over the sync Iter
  (let [m (-> (om/ormap "kb" {:store-config (mem)} so) (om/assoc :k 1 so) (om/assoc :k 2 so))]
    (is (= #{1 2} (om/get m :k so)))))

(deftest orset-twopset-sync                   ; set->clj over both halves + difference
  (let [s (-> (o/orset "r" {:store-config (mem)} so) (o/conj :a so) (o/conj :b so) (o/disj :a so))
        tp (-> (t/twopset "p" {:store-config (mem)} so) (t/conj 1 so) (t/conj 2 so) (t/disj 1 so))]
    (is (= #{:b} (o/elements s so)))
    (is (= #{2} (t/elements tp so)))))
