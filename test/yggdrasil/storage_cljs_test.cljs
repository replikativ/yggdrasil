(ns yggdrasil.storage-cljs-test
  "Runtime validation of storage.cljc's cljs IStorage branch: a PSS sorted-set
   backed by KonserveStorage over an async cljs konserve store, store + restore,
   round-trip. Exercises the cljs node serialization (keys/from-map) + the
   async+sync + konserve->partial-cps bridge end-to-end on ClojureScript/node."
  (:require [cljs.test :refer-macros [deftest is async]]
            [cljs.core.async :as a :refer [go <!]]
            [konserve.memory :refer [new-mem-store]]
            [org.replikativ.persistent-sorted-set :as pss]
            [yggdrasil.storage :as storage]))

(defn- realize
  "Realize a partial-cps CPS fn (fn [resolve reject]) into a promise-chan
   yielding [:ok v] / [:err e]."
  [cps]
  (let [ch (a/promise-chan)]
    (cps (fn [v] (a/put! ch [:ok v]))
         (fn [e] (a/put! ch [:err e])))
    ch))

(deftest pss-konserve-roundtrip
  (async done
    (go
      (let [store (<! (new-mem-store))
            stg   (storage/create-storage store)
            s     (into (pss/sorted-set-by compare) (range 64))
            [tag address] (<! (realize (pss/store s stg {:sync? false})))]
        (is (= :ok tag) "store returned an address via the async IStorage")
        (let [restored        (pss/restore address stg {:sync? false})
              [tag2 ok]       (<! (realize (pss/equiv-sequential? restored (range 64) {:sync? false})))]
          (is (= :ok tag2) "restore + lazy traversal completed async")
          (is (true? ok) "restored set equals the original 0..63 — round-trip through konserve"))
        (done)))))
