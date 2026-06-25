(ns yggdrasil.migrate
  "On-disk store migration from a RELEASED yggdrasil (origin/main) to the current
   layout. The only released durable PSS store is the snapshot **registry** (the
   workspace delegates to it; nothing else persists a PSS index). The convergent
   CRDT catalog (G-Set/OR-Set/CDVCS/…) is unreleased, so there is no CRDT data on
   disk to migrate — only the registry.

   Released registry (origin/main): a SINGLE PSS index of entries under
   `:registry/roots {:tsbs <root>}`, whose nodes are UNTAGGED plain konserve maps
   (`{:level :keys :addresses}`, leaf `:keys` are `entry->map` maps). The current
   registry is a durable **2P-Set** (adds + removals halves) under `:crdt/roots
   {:adds <root> :removals <root>}` — so deregistration is convergent — and its
   nodes are the canonical TAGGED codec (`pss/leaf` / `pss/branch`).

   `migrate-registry-0.2->0.3!` walks the old `:tsbs` index as RAW node maps (an
   untagged map reads back as a map straight through the canonical serializer — no
   PSS restore, no tag), then `register-batch!`s every entry into a fresh registry
   on the SAME store. Because it REBUILDS through the current code, it covers BOTH
   the structure change (single-PSS → 2P-Set) AND the node-format change
   (untagged map → canonical tagged object) in one pass — there is no separate
   node rewrite to do. IDEMPOTENT: a store already on the new layout (no
   `:registry/roots`) is returned untouched. The dead `:registry/roots`/
   `:registry/freed` cells + old nodes are left in place — a windowed
   `registry/gc!` reclaims them (unreachable from `:crdt/roots`). JVM/ops tool (sync)."
  (:require [yggdrasil.kbridge :as kb]
            [yggdrasil.storage :as store]
            [yggdrasil.registry :as registry]))

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
