(ns ^:no-doc yggdrasil.kbridge
  "Konserve-call conveniences for the `async+sync` storage layer. These are NOT a
   bridge — the channel↔CPS bridging lives entirely in `is.simm.partial-cps.core-async`
   (`ca/sync-or-cps`, shared with spindel). Each `k-*` is just `konserve.core/<op>`
   composed with `ca/sync-or-cps`, which on `{:sync? true}` (JVM) passes the konserve
   VALUE through and on `{:sync? false}` (cljs) wraps the konserve channel as an
   `await`-able partial-cps CPS. So one `(await (kb/k-get …))` works on both platforms."
  (:require [konserve.core :as k]
            [is.simm.partial-cps.core-async :as ca]))

(defn k-get [store key opts]
  (ca/sync-or-cps (k/get store key nil opts) opts))

(defn k-get-meta
  "The konserve per-key metadata map (carries `:last-write`, a Date) — nil if absent.
   Used by commit-DAG GC to date a content-addressed root by when it was first stored."
  [store key opts]
  (ca/sync-or-cps (k/get-meta store key nil opts) opts))

(def immutable-meta
  "The metadata map marking a content-addressed, WRITE-ONCE value (a PSS node, a
   commit, an addressable snapshot). Pass as `k-assoc`'s `meta` arg at such call
   sites: a sync peer that already holds the key can then skip re-storing it (no
   write-hook echo). Mutable cells (branch heads / roots pointers) stay UNMARKED —
   they ride the convergent δ path, not the node push."
  {:immutable? true})

(defn k-assoc
  "Write `val` under `key`. The 5-arity threads konserve's per-write metadata channel:
   a `meta` MAP (e.g. `{:immutable? true}` for content-addressed write-once values) is
   merged into the value's stored metadata + forwarded on the write-hook, so a sync
   layer can skip re-storing an immutable value it already has. `nil` meta ≡ the
   4-arity (konserve's own 5-arity collapses to the plain build)."
  ([store key val opts] (k-assoc store key val nil opts))
  ([store key val meta opts]
   (ca/sync-or-cps (k/assoc store key val meta opts) opts)))

(defn k-dissoc [store key opts]
  (ca/sync-or-cps (k/dissoc store key opts) opts))

(defn k-update
  "ATOMIC per-key read-modify-write via konserve's `update` (go-locked — no
   get-then-assoc TOCTOU). `f` receives the current value (nil if absent) and
   returns the new value. Use for convergent grow-cells (roots/freed) so a
   concurrent flush or synced peer can't lose an interleaved write."
  [store key f opts]
  (ca/sync-or-cps (k/update store key f opts) opts))
