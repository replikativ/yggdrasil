(ns yggdrasil.convergent.durable-gset-cljs-test
  "Runtime proof that a durable G-Set runs on ClojureScript over ASYNC konserve.
   The record carries its sync-mode, so EVERY op — functional (factory/add/flush/
   elements) AND the protocol `-join` — is async (CPS) on cljs. Also exercises
   the browser receive→restore→read path over a lazy storage-backed PSS set."
  (:require [cljs.test :refer-macros [deftest is async]]
            [cljs.core.async :as a :refer [go <!]]
            [org.replikativ.persistent-sorted-set :as pss]
            [is.simm.partial-cps.sequence :as aseq]
            [is.simm.partial-cps.async :refer-macros [async] :refer [await]]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-gset :as g]))

(defn- realize [cps]
  (let [ch (a/promise-chan)]
    (cps (fn [v] (a/put! ch [:ok v])) (fn [e] (a/put! ch [:err e])))
    ch))

(defn- drain-set [s]
  (async (loop [items (await (pss/seq s {:sync? false})) acc #{}]
           (if-some [x (await (aseq/first items))]
             (recur (await (aseq/rest items)) (conj acc x))
             acc))))

(deftest durable-gset-async-end-to-end
  (async done
    (go
      (let [sc        {:backend :memory :id (random-uuid)}
            ;; the record opened :sync? false → all its ops are async
            [t1 gs]   (<! (realize (g/durable-gset "t" :store-config sc :sync? false)))
            [t2 gs]   (<! (realize (g/add gs :x)))
            [t3 gs]   (<! (realize (g/add gs :y)))
            [t4 _]    (<! (realize (g/flush! gs)))
            [t5 els]  (<! (realize (g/elements gs)))
            ;; protocol -join is async too (record-carried mode) — idempotent
            [t6 j]    (<! (realize (c/-join gs gs)))
            [t7 jels] (<! (realize (g/elements j)))
            ;; receive→restore→read: a freshly-restored LAZY set
            [_ roots] (<! (realize (d/load-roots (:kv-store gs) {:sync? false})))
            restored  (d/restore-set compare (:main roots) (:storage gs) {:sync? false})
            [t8 els2] (<! (realize (drain-set restored)))]
        (is (every? #(= :ok %) [t1 t2 t3 t4 t5 t6 t7 t8]) "every async op (incl. -join) completed")
        (is (= #{:x :y} els) "async add + read round-trips")
        (is (= #{:x :y} jels) "protocol -join is cross-platform (idempotent here)")
        (is (= #{:x :y} els2) "freshly-restored LAZY set reads the same elements")
        (done)))))
