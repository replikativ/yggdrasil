(ns yggdrasil.wire
  "Standard kabel/Fressian WIRE handlers for yggdrasil — ONE serializer that ships
   PSS nodes + roots AND system VALUES (`ygg/system`), so any yggdrasil CRDT rides the
   SAME socket as datahike/stratum/proximum (all share the `pss-fress` canonical codec
   + storage registry). This is the wire counterpart to `storage/attach-pss-serializer!`
   (which is the per-store, durable serializer): here the resolvers are the shared
   REGISTRY (resolve a received value's storage/comparator by an id the root stamps),
   so many stores' nodes/values cross one wire without the serializer knowing any
   store's live objects. Mirrors `datahike.kabel.fressian-handlers`.

   Usage (assemble into a kabel middleware with `kabel.middleware.fressian/fressian`):

     (require '[yggdrasil.convergent.gset :as g])       ; registers :gset in the codec
     (yggdrasil.wire/register-store! store-id kv-store)  ; per-connect, so reads resolve
     (fressian (atom (yggdrasil.wire/read-handlers))
               (atom (yggdrasil.wire/write-handlers)) peer-config)

   A CONVERGENT value ships as `{plain fields + each PSS field's content-addressed root
   address}` (its `project`); the receiver restores it against the store the nodes were
   synced into (konserve-sync). So `signal-sync` ships the value/δ and `konserve-sync`
   ships the nodes — one codec, `-join`/`-apply-delta` the only per-CRDT bit."
  (:require [org.replikativ.persistent-sorted-set.fressian :as pss-fress]
            [yggdrasil.fressian :as yf]
            [yggdrasil.storage :as store]))

(defn register-store!
  "Register `kv-store` under `store-id` in the shared PSS storage-registry, so a
   received system VALUE (`ygg/system`) or PSS root resolves its storage against it.
   Call per-connect. `store-id` is what a system's `:store-config` `:id` carries (and
   what a PSS root stamps as `:pss/storage-id`)."
  [store-id kv-store]
  (pss-fress/register-storage! store-id (store/create-storage kv-store)))

(defn unregister-store!
  "Drop `store-id` from the shared storage-registry (per-connect teardown)."
  [store-id]
  (pss-fress/unregister-storage! store-id))

(defn- registry-resolve-storage
  "Wire resolver shared by the PSS-root and `ygg/system` read handlers: resolve a
   received value's live IStorage by the id it carries — a system blob's
   `[:store-config :id]`, or a PSS root's `:pss/storage-id` (in `:meta`)."
  [blob]
  (pss-fress/registered-storage
   (or (get-in blob [:store-config :id])
       (get (:meta blob) pss-fress/storage-id-key))))

(defn write-handlers
  "The merged WIRE write-handler map: PSS node + root handlers + every registered
   `ygg/system`. Call AFTER the systems you ship have been required (each registers at
   ns-load). JVM: `{Class {tag WriteHandler}}`; cljs: `{Type fn}`."
  []
  (merge pss-fress/write-handlers
         pss-fress/root-write-handlers
         (yf/write-handlers)))

(defn read-handlers
  "The merged WIRE read-handler map: PSS nodes (storage-free) + PSS root + `ygg/system`,
   the root + system handlers sharing a `resolve-storage`. Defaults to the shared
   registry resolver (`register-store!` populates it); pass `:resolve-storage` to close
   over a single store (lexical). `:sync?` is the mode a reconstructed system runs in."
  ([] (read-handlers {}))
  ([{:keys [resolve-storage sync?] :or {sync? true}}]
   (let [rs (or resolve-storage registry-resolve-storage)]
     (merge (pss-fress/read-handlers {})
            {pss-fress/set-tag (pss-fress/root-read-handler {:resolve-storage rs})}
            (yf/read-handlers {:resolve-storage rs :sync? sync?})))))
