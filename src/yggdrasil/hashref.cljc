(ns ^:no-doc yggdrasil.hashref
  "Fressian read/write handlers for hasch `HashRef` — a content-address POINTER to a
   broken-out value. PURE SERIALIZATION, no IO: the handler only (de)serializes the ref's
   `:hash-bytes`; storing/resolving the referenced value is the break-out helper's job
   (`yggdrasil.cas`).

   A `HashRef` MUST round-trip AS a `HashRef` (not as a plain uuid): a PSS node holding a
   ref keeps the SAME content-address as the inline value (hasch `hash-ref` transparency —
   `(uuid {:x (hash-ref v)}) == (uuid {:x v})`), so break-out is invisible to the node hash
   and dedup/convergence hold. A plain uuid in the element would change the node hash.

   Registered by DEFAULT in every store's serializer (`storage/attach-pss-serializer!`),
   composed alongside the canonical PSS node handlers — the node codec is element-agnostic,
   so element values that are refs recurse through this handler. JVM: `{Class {tag Write
   Handler}}` / `{tag ReadHandler}`; cljs: `{Type fn}` / `{tag fn}` (the shapes konserve's
   FressianSerializer expects, same as the PSS node handlers)."
  (:require [hasch.benc :as benc]
            #?(:cljs [fress.api :as fress]))
  #?(:clj (:import [hasch.benc HashRef]
                   [org.fressian.handlers WriteHandler ReadHandler])))

(def ^:const ref-tag "hasch/ref")

(def write-handlers
  #?(:clj
     {HashRef {ref-tag (reify WriteHandler
                         (write [_ w o] (.writeTag w ref-tag 1) (.writeObject w (:hash-bytes o))))}}
     :cljs
     {benc/HashRef (fn [w o] (fress/write-tag w ref-tag 1) (fress/write-object w (:hash-bytes o)))}))

(def read-handlers
  #?(:clj
     {ref-tag (reify ReadHandler (read [_ rdr _tag _n] (benc/->HashRef (.readObject rdr))))}
     ;; cljs: hasch `:hash-bytes` is a JS Array (`into-array`), but fressian reads the list
     ;; back as a cljs vector, which the SHA digest rejects — normalize to a JS array.
     :cljs
     {ref-tag (fn [rdr _tag _n] (benc/->HashRef (into-array (fress/read-object rdr))))}))
