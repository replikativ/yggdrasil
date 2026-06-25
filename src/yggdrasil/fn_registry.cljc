(ns ^:no-doc yggdrasil.fn-registry
  "Named behavioral functions a system carries but CANNOT serialize — an OR-Set's
   `tag-fn`, a merging-OR-Map's `merge-fn` (and, later, an adapter's connection
   resolver). A function is not a value; you can ship its NAME, not its code.

   So a system that uses a custom such fn is constructed with a registered ID (a
   keyword), stores that id in its `:config` (plain data, serialized for free), and
   resolves the live fn from here on reconstruction. The id is the SINGLE source of
   truth — the runtime fn and the serialized id both derive from it, so they cannot
   diverge (the accidental-symmetry-breaking that a separate fn+id pair invites).

   Both writer and reader must `register-fn!` the same `id -> fn` (the fn is code,
   present on every peer; the id is the shared contract) — exactly as datahike
   registers its index-type comparators everywhere. Default instances pass no id and
   never touch this registry.

   COMPARATORS ARE NOT HERE: they are FIXED per system type (`compare` for the flat
   CRDTs, `graph-cmp` for CDVCS), hardcoded in each system's `reconstruct`. One
   definition shared by construction → nothing to drift, which is the whole reason a
   comparator (which orders the on-disk PSS) must never be registry-resolved."
  (:refer-clojure :exclude [resolve]))

(defonce ^{:doc "id -> fn"} registry (atom {}))

(defn register-fn!
  "Register `f` under `id` (a keyword). Call at ns-load on every peer that
   (de)serializes a system using it. Returns `id`."
  [id f]
  (swap! registry assoc id f)
  id)

(defn resolve-fn
  "The fn registered under `id`, or nil (incl. when `id` is nil)."
  [id]
  (when id (get @registry id)))

(defn registered?
  "Whether `id` names a registered fn."
  [id]
  (contains? @registry id))
