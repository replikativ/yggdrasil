(ns yggdrasil.migrate
  "On-disk store migrations between yggdrasil versions.

   0.2 → 0.3: the snapshot registry's durable layout changed. In 0.2 the registry
   was a SINGLE PSS index stored under `:registry/roots {:tsbs <root>}`; in 0.3 it
   is a durable **2P-Set** (adds + removals halves) under `:crdt/roots
   {:adds <root> :removals <root>}` — so deregistration is convergent. The PSS NODE
   format is unchanged (`{:level :keys :addresses}`, leaf `:keys` are entry-maps),
   so an old tree is still readable; the migration just re-keys it.

   `migrate-registry-0.2->0.3!` walks the old `:tsbs` index (raw node maps — no PSS
   restore needed), then `register-batch!`s every entry into a fresh 0.3 registry on
   the SAME store (writing the new `:crdt/roots`). It is IDEMPOTENT: a store already
   on 0.3 (no `:registry/roots`) is returned untouched, and re-running content-
   addresses to the same 2P-Set. The dead `:registry/roots`/`:registry/freed` cells
   and old nodes are left in place — a windowed `registry/gc!` reclaims them (they
   are unreachable from `:crdt/roots`). JVM/ops tool (sync).

   PRE-CANONICAL NODE FORMAT (≤ 2026-06-19): durable CRDTs stored PSS nodes as
   UNTAGGED plain konserve maps (`{:level :keys :addresses}`); the canonical codec
   (`org.replikativ.persistent-sorted-set.fressian`) stores them as TAGGED objects
   (`pss/leaf` / `pss/branch`). `migrate-store-nodes!` rewrites a pre-canonical
   store's nodes in place — content addresses are STABLE (`node->map` is unchanged),
   so it is an address-preserving rewrite, not a re-key."
  (:require [yggdrasil.kbridge :as kb]
            [yggdrasil.storage :as store]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.registry :as registry])
  (:import [org.replikativ.persistent_sorted_set Leaf Branch Settings]))

(def ^:private sync {:sync? true})

(defn- walk-leaf-entries
  "Collect every leaf key (an entry-map) reachable from `root` by walking the raw
   PSS node maps (`:addresses` = branch children; else `:keys` = leaf entries)."
  [kv-store root]
  (loop [stack [root] acc []]
    (if-let [addr (peek stack)]
      (let [node (kb/k-get kv-store addr sync)]
        (if-let [children (:addresses node)]
          (recur (into (pop stack) children) acc)
          (recur (pop stack) (into acc (:keys node)))))
      acc)))

(defn migrate-registry-0.2->0.3!
  "Migrate a 0.2 registry store at `store-config` to the 0.3 2P-Set layout, in
   place. Returns the migrated `Registry` (already on `:crdt/roots`). No-op +
   returns a fresh-but-empty/already-0.3 registry when there is no `:registry/roots`
   to migrate."
  [store-config]
  (let [reg     (registry/create-registry {:store-config store-config})
        kv      (:kv-store reg)
        old     (kb/k-get kv :registry/roots sync)
        tsbs    (:tsbs old)]
    (when tsbs
      (let [entries (mapv store/map->entry (walk-leaf-entries kv tsbs))]
        (registry/register-batch! reg entries)
        (registry/flush! reg)))
    reg))

;; ============================================================
;; Pre-canonical node-format migration (untagged plain map -> tagged object)
;; ============================================================

(defn- map->node
  "Reconstruct a pre-canonical plain-map PSS node into a canonical Leaf/Branch
   OBJECT, so re-storing it under the canonical serializer writes the tagged
   `pss/leaf` / `pss/branch` blob. A branch map carries `:addresses`; a leaf does
   not. (JVM — migration is a JVM ops tool.)"
  [m ^Settings settings]
  (if (:addresses m)
    (Branch. (int (:level m)) ^java.util.List (:keys m) ^java.util.List (:addresses m) settings)
    (Leaf. ^java.util.List (:keys m) settings)))

(defn- collect-addresses
  "Flatten any `:crdt/roots` cell shape ({branch -> addr}, or the 2P-Set
   {:adds addr :removals addr}, possibly nested per branch) to its root-address
   leaves (strings / uuids)."
  [x]
  (cond
    (map? x)        (mapcat collect-addresses (vals x))
    (sequential? x) (mapcat collect-addresses x)
    (or (string? x) (uuid? x)) [x]
    :else []))

(defn migrate-store-nodes!
  "Rewrite every PSS node in a pre-canonical durable store (untagged plain-map
   nodes) under the canonical tagged codec, IN PLACE — content addresses are
   stable (`node->map` is unchanged), so this preserves addresses rather than
   re-keying. Reachable nodes are found from each root in the store's `:crdt/roots`
   cell (the `reachable-addresses` walk follows the plain maps' `:addresses`).
   Idempotent: a node already in canonical (object) form is skipped, so a fully
   canonical store rewrites nothing. For CRDTs whose elements aren't fressian-native
   (e.g. the registry's RegistryEntry) pass `:element-read-handlers` /
   `:element-write-handlers` (threaded to the store open). Returns the number of
   nodes rewritten. JVM/ops tool (sync)."
  ([store-config] (migrate-store-nodes! store-config {}))
  ([store-config opts]
   (let [opts     (merge sync opts)
         {:keys [kv-store]} (d/open store-config opts)
         settings (store/default-settings)
         roots    (d/load-roots kv-store opts)
         addrs    (into #{} (mapcat #(d/reachable-addresses kv-store % opts)
                                    (collect-addresses roots)))]
     (reduce (fn [n addr]
               (let [v (kb/k-get kv-store addr opts)]
                 (if (map? v)              ; a canonical node is an object, not a map
                   (do (kb/k-assoc kv-store addr (map->node v settings) opts) (inc n))
                   n)))
             0 addrs))))
