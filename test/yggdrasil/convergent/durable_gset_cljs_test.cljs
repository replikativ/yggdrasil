(ns yggdrasil.convergent.durable-gset-cljs-test
  "Runtime proof that a durable G-Set runs on ClojureScript over ASYNC konserve:
   the factory + add + flush + elements are partial-cps async (CPS), and a
   freshly-RESTORED lazy PSS set materializes via the async walk. This is the
   browser-side local-first path (receive nodes → restore → read; edit → flush)."
  (:require [cljs.test :refer-macros [deftest is async]]
            [cljs.core.async :as a :refer [go <!]]
            [org.replikativ.persistent-sorted-set :as pss]
            [is.simm.partial-cps.sequence :as aseq]
            [is.simm.partial-cps.async :refer-macros [async] :refer [await]]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-gset :as g]))

(defn- realize
  "Realize a partial-cps CPS fn into a promise-chan yielding [:ok v] / [:err e]."
  [cps]
  (let [ch (a/promise-chan)]
    (cps (fn [v] (a/put! ch [:ok v])) (fn [e] (a/put! ch [:err e])))
    ch))

(def ^:private aopts {:sync? false})

(defn- drain-set
  "Materialize a (lazy) PSS set into a Clojure set via the async-seq walk."
  [s]
  (async (loop [items (await (pss/seq s aopts)) acc #{}]
           (if-some [x (await (aseq/first items))]
             (recur (await (aseq/rest items)) (conj acc x))
             acc))))

(deftest durable-gset-async-write-and-read
  (async done
    (go
      (let [sc        {:backend :memory :id (random-uuid)}
            [t1 gs]   (<! (realize (g/durable-gset "t" :store-config sc :sync? false)))
            [t2 gs]   (<! (realize (g/add gs :x aopts)))
            [t3 gs]   (<! (realize (g/add gs :y aopts)))
            [t4 _]    (<! (realize (g/flush! gs aopts)))
            [t5 els]  (<! (realize (g/elements gs aopts)))
            ;; the browser receive→restore→read path: restore a LAZY set from the
            ;; flushed root and materialize it via the async-seq walk.
            [_ roots] (<! (realize (d/load-roots (:kv-store gs) aopts)))
            restored  (d/restore-set compare (:main roots) (:storage gs) aopts)
            [t6 els2] (<! (realize (drain-set restored)))]
        (is (every? #(= :ok %) [t1 t2 t3 t4 t5 t6]) "every async op completed")
        (is (= #{:x :y} els) "async add + read round-trips")
        (is (= #{:x :y} els2) "freshly-restored LAZY set reads the same elements")
        (done)))))
