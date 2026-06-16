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
   are unreachable from `:crdt/roots`). JVM/ops tool (sync)."
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
