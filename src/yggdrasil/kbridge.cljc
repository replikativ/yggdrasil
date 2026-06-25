(ns ^:no-doc yggdrasil.kbridge
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

(defn await-chan
  "Bridge a RAW konserve/core.async channel (e.g. `konserve.gc/sweep!`, which is
   not :sync?-aware) into `async+sync`: on JVM-sync, block and return the value
   (throw on error); on async, a partial-cps CPS fn over the channel."
  [ch opts]
  (if (:sync? opts)
    #?(:clj  (let [v (clojure.core.async/<!! ch)]
               (if (instance? Throwable v) (throw v) v))
       :cljs ch)
    (chan->cps ch)))

(defn sync-or-cps
  "Bridge a :sync?-aware konserve result into `async+sync`. `result` is what a
   konserve fn returned: a VALUE on `{:sync? true}` (pass through) or a core.async
   CHANNEL on `{:sync? false}` (wrap as a CPS fn). Use for store-lifecycle calls
   (`konserve.store/connect-store` etc.):
     (await (kb/sync-or-cps (kstore/connect-store cfg opts) opts))"
  [result opts]
  (if (:sync? opts) result (chan->cps result)))

(defn k-get [store key opts]
  (if (:sync? opts)
    (k/get store key nil opts)
    (chan->cps (k/get store key nil opts))))

(def immutable-meta
  "The metadata map marking a content-addressed, WRITE-ONCE value (a PSS node, a
   commit, an addressable snapshot). Pass as `k-assoc`'s `meta` arg at such call
   sites: a sync peer that already holds the key can then skip re-storing it (no
   write-hook echo). Mutable cells (branch heads / roots pointers) stay UNMARKED —
   they ride the convergent δ path, not the node push."
  {:immutable? true})

(defn k-assoc
  "Write `val` under `key`. The 5-arity threads konserve's per-write metadata channel:
   a `meta` MAP (e.g. `{:immutable? true}` for content-addressed write-once values) is
   merged into the value's stored metadata + forwarded on the write-hook, so a sync
   layer can skip re-storing an immutable value it already has. `nil` meta ≡ the
   4-arity (konserve's own 5-arity collapses to the plain build)."
  ([store key val opts] (k-assoc store key val nil opts))
  ([store key val meta opts]
   (if (:sync? opts)
     (k/assoc store key val meta opts)
     (chan->cps (k/assoc store key val meta opts)))))

(defn k-dissoc [store key opts]
  (if (:sync? opts)
    (k/dissoc store key opts)
    (chan->cps (k/dissoc store key opts))))

(defn k-update
  "ATOMIC per-key read-modify-write via konserve's `update` (go-locked — no
   get-then-assoc TOCTOU). `f` receives the current value (nil if absent) and
   returns the new value. Use for convergent grow-cells (roots/freed) so a
   concurrent flush or synced peer can't lose an interleaved write."
  [store key f opts]
  (if (:sync? opts)
    (k/update store key f opts)
    (chan->cps (k/update store key f opts))))
