(ns yggdrasil.kbridge
  "Bridge konserve to partial-cps `await` for the storage layer's `async+sync`.

   `k-get`/`k-assoc`/`k-dissoc` dispatch on `:sync?`:
   - `{:sync? true}` (JVM) → the konserve VALUE directly, so under `async+sync`
     sync-mode (`await` → `do`) the awaited expression yields the value.
   - `{:sync? false}` (cljs) → a **partial-cps CPS fn** `(fn [resolve reject] …)`
     over konserve's core.async channel, so `await` suspends correctly.

   This is the correctness-critical shape: a naive `(await (chan->cps (k/get …)))`
   would break sync-mode, because `chan->cps` returns a CPS fn but `await`→`do`
   needs the expression to be the value. Dispatching inside `k-get` fixes it."
  (:require [konserve.core :as k]
            #?(:clj  [clojure.core.async :refer [take!]]
               :cljs [cljs.core.async :refer [take!]])))

(defn- chan->cps
  "Wrap a konserve core.async channel as a partial-cps CPS fn."
  [ch]
  (fn [resolve reject]
    (take! ch (fn [v]
                (if (instance? #?(:clj Throwable :cljs js/Error) v)
                  (reject v)
                  (resolve v))))))

(defn k-get [store key opts]
  (if (:sync? opts)
    (k/get store key nil opts)
    (chan->cps (k/get store key nil opts))))

(defn k-assoc [store key val opts]
  (if (:sync? opts)
    (k/assoc store key val opts)
    (chan->cps (k/assoc store key val opts))))

(defn k-dissoc [store key opts]
  (if (:sync? opts)
    (k/dissoc store key opts)
    (chan->cps (k/dissoc store key opts))))
