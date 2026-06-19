(ns yggdrasil.registry
  "Cross-system snapshot registry — a query lens over a durable 2P-Set of
   RegistryEntry (`yggdrasil.convergent.twopset`).

   The registry is a conflict-free yggdrasil system: it grows and reconciles by
   union, but also needs convergent REMOVE (deregister a head, GC an old
   snapshot), which is exactly a **2P-Set** — two grow-only, content-addressed
   PSS sets (adds + removals), live = adds − removals. Content-addressing makes
   register! idempotent and a deregistered entry stay gone (a GC'd snapshot's
   content is gone; new work is a new snapshot-id, never a dead re-add).

   This namespace is the registry-flavored projection: RegistryEntry elements,
   the tsbs order (`[hlc system-id branch-name snapshot-id]`), and the temporal /
   per-system queries. The CRDT mechanics (durability, -join, sync via
   konserve-sync, system-hood) all come from the 2P-Set underneath — which is a
   first-class yggdrasil system (`registry-system`), so another yggdrasil can
   itself track/fork/merge a registry.

   Entries serialize via the 2P-Set's element codec (`entry->map`/`map->entry`):
   bare-element keys, so the codec applies cleanly (no `[elem tag]` pairs).
   Queries full-scan the live set — fast enough at the expected scale."
  (:require [yggdrasil.types :as t]
            [yggdrasil.storage :as store]
            [yggdrasil.convergent.twopset :as d2p])
  #?(:clj (:import [org.fressian.handlers WriteHandler ReadHandler]
                   [yggdrasil.types RegistryEntry])))

;; RegistryEntry is a JVM record → it needs a fressian element handler so durable
;; (file/IndexedDB) stores serialize the registry's node keys via the canonical PSS
;; node handlers (which recurse into element values). cljs registry entries are plain
;; maps (fressian-native), so no handler there. The codec is the existing
;; entry->map / map->entry (storage).
#?(:clj
   (def ^:private element-write-handlers
     {RegistryEntry
      {"yggdrasil/registry-entry"
       (reify WriteHandler
         (write [_ w e]
           (.writeTag w "yggdrasil/registry-entry" 1)
           (.writeObject w (store/entry->map e))))}}))

#?(:clj
   (def ^:private element-read-handlers
     {"yggdrasil/registry-entry"
      (reify ReadHandler
        (read [_ rdr _tag _n] (store/map->entry (.readObject rdr))))}))

;; ============================================================
;; tsbs comparator
;; ============================================================

(defn- safe-compare [a b]
  (cond (and (nil? a) (nil? b)) 0
        (nil? a) -1 (nil? b) 1
        :else (compare a b)))

(defn- hlc-cmp [a b]
  (cond (and (nil? a) (nil? b)) 0
        (nil? a) -1 (nil? b) 1
        :else (t/hlc-compare a b)))

(def ^:private tsbs-comparator
  "Sort by [hlc system-id branch-name snapshot-id]."
  (fn [a b]
    (let [c (hlc-cmp (:hlc a) (:hlc b))]
      (if-not (zero? c) c
              (let [c (safe-compare (:system-id a) (:system-id b))]
                (if-not (zero? c) c
                        (let [c (safe-compare (:branch-name a) (:branch-name b))]
                          (if-not (zero? c) c
                                  (safe-compare (:snapshot-id a) (:snapshot-id b))))))))))

;; ============================================================
;; Registry — a lens over a durable 2P-Set
;; ============================================================

;; The registry is a CONN: a stable handle whose `tpset-atom` holds the durable
;; 2P-Set VALUE (value-semantic — every d2p op returns a new value, which the
;; mutators `swap!` in). The one mutable cell is this atom (the service boundary,
;; like a datahike conn) — the CRDT it holds carries no internal mutable state.
;; The external API (register!/deregister!/flush!/queries) is unchanged, so
;; callers (workspace/gc) are unaffected by the value-semantic conversion.
(defrecord Registry [tpset-atom kv-store store-config])

(defn registry-system
  "A SNAPSHOT of the underlying durable 2P-Set value — a first-class conflict-free
   yggdrasil system (SystemIdentity/Snapshotable/Branchable/Mergeable/PConvergent).
   This is what another yggdrasil (a composite or meta-registry) tracks, forks, or
   merges: the registry, being content-addressed, has a stable snapshot identity."
  [^Registry registry]
  @(:tpset-atom registry))

(defn- live-entries [^Registry registry]
  (d2p/elements @(:tpset-atom registry)))

;; ============================================================
;; CRUD
;; ============================================================

(defn register!
  "Add a RegistryEntry (idempotent — content-addressed)."
  [^Registry registry entry]
  (swap! (:tpset-atom registry) #(d2p/conj % entry))
  registry)

(defn register-batch!
  "Register multiple entries at once."
  [^Registry registry entries]
  (swap! (:tpset-atom registry) #(d2p/into % entries))
  registry)

(defn deregister!
  "Convergent observed-remove: tombstone the entry (permanent per content)."
  [^Registry registry entry]
  (swap! (:tpset-atom registry) #(d2p/disj % entry))
  registry)

(defn flush!
  "Persist the 2P-Set (both halves + the :crdt/roots cell + freed) and adopt the
   flushed (dirty-cleared) value back into the conn."
  [^Registry registry]
  (swap! (:tpset-atom registry) d2p/flush!)
  registry)

;; ============================================================
;; Query — temporal
;; ============================================================

(defn as-of
  "World state at time T: latest live entry per [system-id branch-name] with
   HLC <= T. Result: {[system-id branch-name] -> RegistryEntry}."
  [^Registry registry hlc]
  (->> (live-entries registry)
       (filter #(<= (hlc-cmp (:hlc %) hlc) 0))
       (sort tsbs-comparator)
       (group-by (juxt :system-id :branch-name))
       (reduce-kv (fn [m k vs] (assoc m k (last vs))) {})))

(defn entries-in-range
  "All live entries with HLC between from-hlc and to-hlc (inclusive)."
  [^Registry registry from-hlc to-hlc]
  (seq (->> (live-entries registry)
            (filter #(and (<= (hlc-cmp from-hlc (:hlc %)) 0)
                          (<= (hlc-cmp (:hlc %) to-hlc) 0)))
            (sort tsbs-comparator))))

;; ============================================================
;; Query — per-system history
;; ============================================================

(defn system-history
  "History for a system+branch, newest first. opts: {:limit n :since snapshot-id}."
  ([^Registry registry system-id branch-name]
   (system-history registry system-id branch-name {}))
  ([^Registry registry system-id branch-name opts]
   (cond->> (->> (live-entries registry)
                 (filter #(and (= (:system-id %) system-id)
                               (= (:branch-name %) branch-name)))
                 (sort tsbs-comparator)
                 reverse)
     (:limit opts) (take (:limit opts))
     (:since opts) (take-while #(not= (:snapshot-id %) (:since opts))))))

(defn system-branches
  "All branches known for a given system-id."
  [^Registry registry system-id]
  (into #{} (comp (filter #(= (:system-id %) system-id)) (map :branch-name))
        (live-entries registry)))

;; ============================================================
;; Query — snapshot refs
;; ============================================================

(defn snapshot-refs
  "All live references to a given snapshot-id, or nil if none."
  [^Registry registry snapshot-id]
  (seq (filter #(= (:snapshot-id %) snapshot-id) (live-entries registry))))

(defn snapshot-systems
  "Which systems reference a given snapshot-id (set of system-id)."
  [^Registry registry snapshot-id]
  (into #{} (map :system-id) (or (snapshot-refs registry snapshot-id) [])))

;; ============================================================
;; Bulk
;; ============================================================

(defn all-entries [^Registry registry] (live-entries registry))

(defn entry-count [^Registry registry] (count (live-entries registry)))

(defn max-hlc
  "Maximum HLC among live entries, or nil if empty."
  [^Registry registry]
  (let [live (live-entries registry)]
    (when (seq live)
      (:hlc (last (sort-by :hlc hlc-cmp live))))))

;; ============================================================
;; Factory
;; ============================================================

(defn create-registry
  "Create a registry.

     {:store-config {:backend :file   :id (random-uuid) :path \"/tmp/reg\"}}
     {:store-config {:backend :memory :id (random-uuid)}}
     {:ephemeral true}   ; a fresh in-memory konserve store

   A no-arg call is ephemeral."
  ([] (create-registry {:ephemeral true}))
  ([opts]
   (let [store-config (cond
                        (:store-config opts) (:store-config opts)
                        (:store-path opts) {:backend :file :id (random-uuid)
                                            :path (:store-path opts)}
                        (:ephemeral opts) {:backend :memory :id (random-uuid)}
                        :else (throw (ex-info
                                      (str "Registry requires an explicit persistence choice: "
                                           "{:store-config …} | {:ephemeral true}")
                                      {:opts opts})))
         tpset (d2p/twopset "registry"
                            :store-config store-config
                            :comparator tsbs-comparator
                            :element-read-handlers  #?(:clj element-read-handlers  :cljs nil)
                            :element-write-handlers #?(:clj element-write-handlers :cljs nil))]
     (->Registry (atom tpset) (:kv-store tpset) store-config))))

(defn gc!
  "Reclaim PSS B-tree nodes superseded by prior flushes (mark-and-sweep over the
   underlying 2P-Set). Tombstoned (deregistered) entries remain — they're live
   2P-Set members; this reclaims the old index-tree versions. Returns the set of
   deleted node keys."
  ([^Registry registry] (d2p/gc! @(:tpset-atom registry)))
  ([^Registry registry opts] (d2p/gc! @(:tpset-atom registry) opts)))

(defn close!
  "Flush and close the registry."
  [^Registry registry]
  (flush! registry)
  (when (and (:kv-store registry) (:store-config registry))
    (store/close! (:kv-store registry) (:store-config registry))))
