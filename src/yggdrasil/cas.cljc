(ns yggdrasil.cas
  "Content-addressed break-out for commit/element payloads: store a large value under its
   content hash (write-once) and reference it by a hasch `HashRef`. The ref is TRANSPARENT
   to any enclosing content-hash (`(uuid {:x (hash-ref v)}) == (uuid {:x v})`), so a node or
   commit that inlines the ref keeps the SAME address as one inlining the value — break-out
   is invisible to dedup/convergence. Small values ride inline; the break-out POLICY (when
   to break out) is the consumer's. `yggdrasil.hashref` registers the wire codec (a ref is
   pure data — no IO); GC follows a ref to its key via `hasch/ref->uuid`."
  (:refer-clojure :exclude [resolve])
  (:require [hasch.core :as hasch]
            [yggdrasil.kbridge :as kb]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(defn break-out!
  "Store `value` under its content-address (write-once, `:immutable?`) and return a `HashRef`
   pointer to it. Transparent — an enclosing content-hash is unchanged whether it holds the
   value or this ref. (async+sync)"
  ([kv-store value] (break-out! kv-store value {:sync? true}))
  ([kv-store value opts]
   (async+sync (:sync? opts)
               (async
                (await (kb/k-assoc kv-store (hasch/uuid value) value kb/immutable-meta opts))
                (hasch/hash-ref value)))))

(defn resolve
  "Fetch the value a `HashRef` points to (its content-addressed store entry), or nil.
   (async+sync)"
  ([kv-store ref] (resolve kv-store ref {:sync? true}))
  ([kv-store ref opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-get kv-store (hasch/ref->uuid ref) opts))))))

(defn break-out-if
  "Policy (a): break `value` out when `(size-fn value)` exceeds `threshold`, returning a
   `HashRef`; else return the value inline. `size-fn` defaults to a cheap `pr-str` char
   count (pass a real byte measure for accuracy). (async+sync)"
  ([kv-store value threshold] (break-out-if kv-store value threshold {:sync? true}))
  ([kv-store value threshold opts]
   (let [size-fn (:size-fn opts (comp count pr-str))]
     (async+sync (:sync? opts)
                 (async
                  (if (> (size-fn value) threshold)
                    (await (break-out! kv-store value opts))
                    value))))))
