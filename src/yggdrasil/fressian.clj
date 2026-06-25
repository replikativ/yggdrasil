(ns ^:no-doc yggdrasil.fressian
  "Optional Fressian handlers that serialize a yggdrasil SYSTEM **as a value**, the
   way persistent-sorted-set serializes a set: the plain-data fields are written
   verbatim and every PSS-backed field is written as its content-addressed ROOT
   ADDRESS — a reference, not the inlined elements — so structurally-shared systems
   still dedup through the store's nodes.

   A system record is almost entirely a value; only three fields are runtime, none
   of them part of the value:
     - `storage`/`kv-store` — re-DERIVED from the read context (the store you read
       from), via the SAME `:resolve-storage` PSS uses (a `KonserveStorage` carries
       its `:kv-store`, so kv-store is `(:kv-store storage)` — one resolution);
     - `comparator` — known to the SYSTEM (a G-Set's roots use `compare`; a CDVCS's
       graph uses its own `graph-cmp`, which `slice` reads off the set), so each
       system's `reconstruct` restores its PSS fields with the right comparator
       rather than relying on a global comparator registry.

   So this ns reuses PSS's storage resolver wholesale and owns only a static
   `stype -> {:project :reconstruct}` dispatch table (the analog of a multimethod),
   which each system populates at ns-load. Lexical (one store: close over its
   resolver, threaded by `attach-pss-serializer!`) vs wire (stamp a store-id, resolve
   by registry) is inherited from PSS verbatim.

   CONSTRAINT: `project` realizes the PSS root addresses synchronously (it must flush
   the sets), so the write handler serializes a `:sync? true` system; flush a durable
   `:sync? false` one first — same as pss-fress's \"root must be flushed before
   serialization\"."
  (:require [yggdrasil.protocols :as p])
  (:import [org.fressian.handlers WriteHandler ReadHandler]))

(def ^:const system-tag "ygg/system")

;; stype -> {:class       <record class, keys the write handler>
;;           :project     (fn [system]                 -> plain-data blob; PSS fields
;;                                                         as content-addressed root
;;                                                         addresses; must flush)
;;           :reconstruct (fn [blob storage opts]       -> a live system record; PSS
;;                                                         fields restored with the
;;                                                         system's own comparator,
;;                                                         storage/kv-store re-derived)}
(defonce system-registry (atom {}))

(defn register-system!
  "Register a system type with the value codec. Each system calls this at ns-load
   (the analog of a defmethod). `class` keys the JVM write handler."
  [stype class project reconstruct]
  (swap! system-registry assoc stype {:class class :project project :reconstruct reconstruct})
  stype)

(def ^:private system-write-handler
  (reify WriteHandler
    (write [_ w sys]
      (let [stype (p/system-type sys)
            {:keys [project]} (get @system-registry stype)]
        (when (nil? project)
          (throw (ex-info "No project fn registered for system type" {:stype stype})))
        (.writeTag w system-tag 1)
        (.writeObject w (assoc (project sys) :ygg/stype stype))))))

(defn write-handlers
  "`{Class {tag WriteHandler}}` for every registered system — pass as
   `element-write-handlers` to `yggdrasil.storage/attach-pss-serializer!`."
  []
  (into {} (map (fn [[_ {:keys [class]}]] [class {system-tag system-write-handler}]))
        @system-registry))

(defn read-handlers
  "`{tag ReadHandler}` for `ygg/system`. `ctx`:
     :resolve-storage  (fn [blob] -> IStorage) — the SAME resolver PSS uses; lexical
                       (close over one store, threaded by attach-pss-serializer!) or
                       a registry resolver keyed by a stamped store-id. kv-store is
                       derived as `(:kv-store storage)`.
     :sync?            the runtime mode the reconstructed record operates in (default true)."
  [{:keys [resolve-storage sync?] :or {sync? true}}]
  {system-tag
   (reify ReadHandler
     (read [_ rdr _tag _n]
       (let [blob  (.readObject rdr)
             stype (:ygg/stype blob)
             {:keys [reconstruct]} (get @system-registry stype)]
         (when (nil? reconstruct)
           (throw (ex-info "No reconstruct fn registered for system type" {:stype stype})))
         (reconstruct blob (resolve-storage blob) {:sync? sync?}))))})
