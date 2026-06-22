(ns yggdrasil.fressian
  "Optional Fressian handlers that serialize a yggdrasil SYSTEM as a thin, content-
   addressed SNAPSHOT REFERENCE — the system-level analog of persistent-sorted-set's
   `pss/set` root handler.

   A yggdrasil system is `Snapshotable`, so its serialized form is just its snapshot
   handle + the metadata to reopen it: `{:stype :id :snapshot :config}`. The actual
   nodes/blobs stay in storage (content-addressed, shared) — exactly like `pss/set`
   serializes a root reference, not the elements. On read the non-serializable context
   (the konserve store) is supplied by the consumer's `:resolve-store` (lexical: close
   over one store; wire: resolve by an id stamped in the blob), and the per-stype
   `reopen` fn — registered by each system at ns-load, the analog of pss-fress's
   comparator-registry — restores a LIVE system positioned at that snapshot.

   Mirrors pss-fress: ONE tag (`ygg/system`), a runtime registry for the non-
   serializable bits, parameterized read-handlers. JVM prototype; cljs is the same
   shape as the pss-fress node handlers (a `{Type fn}` map).

   CONSTRAINT: the write handler is synchronous and `snapshot-id` realizes the handle
   synchronously only for a `:sync? true` system (JVM / in-mem). A `:sync? false`
   durable system must carry an already-realized handle (flush first) — same flavour as
   pss-fress's \"root MUST be flushed before serialization\"."
  (:require [yggdrasil.protocols :as p])
  (:import [org.fressian.handlers WriteHandler ReadHandler]))

(def ^:const system-tag "ygg/system")

;; stype -> {:class <record class, keys the write handler> :reopen <fn>}
;; reopen = (fn [id config snapshot store opts] -> a LIVE system positioned at snapshot)
(defonce system-registry (atom {}))

(defn register-system!
  "Register a system type for serialization. `class` keys the JVM write handler;
   `reopen` reconstructs a live system at a snapshot. Each system calls this at
   ns-load (like pss-fress consumers register their comparator)."
  [stype class reopen]
  (swap! system-registry assoc stype {:class class :reopen reopen})
  stype)

(defn- system->blob
  "Project a system to its serializable snapshot reference. `snapshot-id` realizes the
   content handle (sync for `:sync? true`); nodes are NOT inlined."
  [sys]
  {:stype    (p/system-type sys)
   :id       (p/system-id sys)
   :snapshot (p/snapshot-id sys)
   :config   (:config sys)})

(def ^:private system-write-handler
  (reify WriteHandler
    (write [_ w sys]
      (.writeTag w system-tag 1)
      (.writeObject w (system->blob sys)))))

(defn write-handlers
  "`{Class {tag WriteHandler}}` covering every registered system type — pass as
   `element-write-handlers` to `yggdrasil.storage/attach-pss-serializer!`, or merge
   into a fressian writer's handler lookup."
  []
  (into {} (map (fn [[_ {:keys [class]}]] [class {system-tag system-write-handler}]))
        @system-registry))

(defn read-handlers
  "`{tag ReadHandler}` for `ygg/system`. `ctx`:
     :resolve-store  (fn [blob] -> konserve kv-store) — the store the system's nodes
                     live in (lexical: `(constantly store)`; wire: resolve by an id in
                     the blob/`:config`).
     :sync?          runtime mode passed to the reopen fn (default true)."
  [{:keys [resolve-store sync?] :or {sync? true}}]
  {system-tag
   (reify ReadHandler
     (read [_ rdr _tag _n]
       (let [{:keys [stype id snapshot config] :as blob} (.readObject rdr)
             {:keys [reopen]} (get @system-registry stype)]
         (when (nil? reopen)
           (throw (ex-info "No reopen fn registered for system type" {:stype stype})))
         (reopen id config snapshot (resolve-store blob) {:sync? sync?}))))})
