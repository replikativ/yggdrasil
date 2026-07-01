(ns yggdrasil.convergent.ormap
  "Observed-Remove Map (OR-Map) — and its merging variant — as a DURABLE
   conflict-free yggdrasil system, on the SAME PSS+konserve substrate as the
   durable sets, READ ON THE FLY (datahike-style): the value is NEVER materialized
   in memory; a key read is a PSS range SLICE (lazy + node-cached), not a full drain.

   FLATTENED LAYOUT: the nested `{k {uid v}}` of the in-memory `ormap` is flattened
   into a grow-only PSS set of `[hk uid k v]` ENTRIES, where `hk = (hasch/uuid k)`.
   Entries are ordered by the DEFAULT `compare` over the 4-vector — which orders by
   `hk` first (so a key's entries are CONTIGUOUS ⇒ range-sliceable), then by `uid`
   (a globally-unique uuid). Because uid is unique, two DISTINCT entries never tie on
   `[hk uid]`, so `compare` never reaches the `k`/`v` positions — hence ANY key and
   value type is storable (the comparator never compares them). A read for key `k`
   slices `[hk MIN-UUID]…[hk MAX-UUID]` — O(log n + matches) nodes, then filters
   stored-`k = k` (hash-collision safety) and diffs the removals slice by uid.

   So a durable OR-Map is structurally an `orset` of entries (two halves
   `:adds`/`:removals` under `:crdt/roots`, add-wins, `set-union` join); the ONLY
   differences are the flattened entry + the SLICE-based per-key projection.

   ONE record serves both maps via `merge-fn`:
   - merge-fn = nil → plain OR-Map: `get k` returns the live value-SET.
   - merge-fn ≠ nil → Merging-OR-Map: `get k` FOLDS the live values via merge-fn
     (wrapped to absorb nils; must be commutative/associative/idempotent).

   `assoc` ALWAYS mints a FRESH uid (re-assoc accumulates, like plain OR-Map); the
   merging variant folds at READ — value-equivalent to replikativ's in-place uid-
   reuse but correct in an append-only PSS. `keys` / `as-of` are full scans (listing
   everything is inherently O(n)); only the per-key `get`/`dissoc` slice.

   **Fully cross-platform**: the record does NOT carry an execution mode — each
   content-touching op takes an OPTIONAL trailing `opts` ({:sync?}, default
   `c/default-opts`: sync on JVM, CPS on cljs)."
  (:refer-clojure :exclude [assoc dissoc get keys])
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.overlay :as ovl]
            [hasch.core :as hasch]
            [yggdrasil.fn-registry :as fr]
            [yggdrasil.fressian :as yf]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->ORMap flush! apply-delta)

;; The sort prefix (hk) and uid are stored as STRINGS, not uuids: default `compare`
;; then orders them LEXICALLY — identical on JVM and cljs. (uuid OBJECTS compare by
;; SIGNED 64-bit longs on the JVM — so `ffff…` is NEGATIVE, not the max — but by
;; STRING on cljs: divergent orders + no usable min/max sentinel. Strings dodge it.)
(def ^:private min-str "00000000-0000-0000-0000-000000000000")   ; lexical min uuid-string
(def ^:private max-str "ffffffff-ffff-ffff-ffff-ffffffffffff")   ; lexical max uuid-string
(defn- hk [k] (str (hasch/uuid k)))           ; content-hash sort prefix of key k (string)
;; Slice bounds MUST be the SAME LENGTH as entries — Clojure vector `compare` is
;; LENGTH-FIRST, so a short bound sorts before every full entry (empty slice). The
;; uid sentinel at position 1 always decides (real uid-strings sit strictly between
;; min-str and max-str), so the nil k/v placeholders at 2-3 are never compared.
(defn- key-lo [k] [(hk k) min-str nil nil])
(defn- key-hi [k] [(hk k) max-str nil nil])

(defn- wrap
  "Absorb nils so a merge-fn seeds the read-time reduce."
  [merge-fn]
  (when merge-fn (fn [a b] (cond (nil? a) b (nil? b) a :else (merge-fn a b)))))

(defn- half-accrue [a b] (merge-with into a b))

(defn- live-for-key
  "async+sync: the live `[hk uid k v]` entries for key `k` — a SLICE of its
   contiguous range in both halves (NOT a full drain), removals diffed by uid, and
   filtered to stored-k = k (hash-collision safety)."
  [o k opts]
  (async+sync (:sync? opts)
              (async
               (let [lo (key-lo k) hi (key-hi k)
                     adds (await (d/slice->clj (:adds o) lo hi opts))
                     rems (await (d/slice->clj (:removals o) lo hi opts))
                     rm-uids (into #{} (map #(nth % 1)) rems)]
                 (->> adds
                      (remove (fn [t] (rm-uids (nth t 1))))
                      (filter (fn [t] (= k (nth t 2)))))))))

(defn- all-live
  "async+sync: EVERY live entry (full scan — for `keys`/`as-of`, inherently O(n))."
  [o opts]
  (async+sync (:sync? opts)
              (async
               (let [adds (await (d/set->clj (:adds o) opts))
                     rems (await (d/set->clj (:removals o) opts))
                     rm-uids (into #{} (map #(nth % 1)) rems)]
                 (remove (fn [t] (rm-uids (nth t 1))) adds)))))

;; ============================================================
;; Record
;; ============================================================

(defrecord ORMap
           [id kv-store store-config storage comparator merge-fn
            adds        ; CURRENT branch's immutable PSS set of [hk uid k v]
            removals    ; CURRENT branch's immutable PSS set of [hk uid k v]
            dirty
            branch      ; current branch keyword
            commit      ; branch TIP commit-id (string) in the commit-DAG; nil before base seed
            config]     ; DOMAIN: cell-keys (:branches-key/:cell-ns); {} ⇒ store defaults

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] (if merge-fn :merging-ormap :ormap))
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true :overlayable true :graphable true})

  p/Snapshotable
  ;; shared two-half snapshot; `as-of` projects the FROZEN map {k value-or-set}.
  (snapshot-id [this] (d/two-half-snapshot-id this c/default-opts))
  (parent-ids [_] (if commit #{commit} #{}))
  (as-of [this snap-id] (p/as-of this snap-id c/default-opts))
  (as-of [this snap-id opts]
    (async+sync (:sync? opts)
                (async
                 (let [[adds* rems*] (await (d/two-half-restore-halves this snap-id opts))
                       rm-uids (into #{} (map #(nth % 1)) rems*)
                       live    (remove (fn [t] (rm-uids (nth t 1))) adds*)]
                   (reduce (fn [m [_hk _uid k v]]
                             (if merge-fn (update m k merge-fn v) (update m k (fnil conj #{}) v)))
                           {} live)))))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  p/Branchable
  ;; store-backed per-branch head cells (shared two-half machinery).
  (branches [this] (d/two-half-branches this c/default-opts))
  (branches [this opts] (d/two-half-branches this opts))
  (current-branch [_] branch)
  (branch! [this name] (d/two-half-branch! this name branch c/default-opts))
  (branch! [this name from] (d/two-half-branch! this name from c/default-opts))
  (branch! [this name from opts] (d/two-half-branch! this name from opts))
  (delete-branch! [this name] (d/two-half-delete-branch! this name c/default-opts))
  (delete-branch! [this name opts] (d/two-half-delete-branch! this name opts))
  (checkout [this name] (d/two-half-checkout this name c/default-opts))
  (checkout [this name opts] (d/two-half-checkout this name opts))

  p/Mergeable
  (merge! [this source] (d/two-half-merge! this source c/default-opts))
  (merge! [this source opts] (d/two-half-merge! this source opts))
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {})

  p/Committable
  ;; commit! DECOUPLED from flush!: materialize both halves + append a commit + advance the tip.
  (commit! [this] (d/two-half-commit! this c/default-opts))
  (commit! [this _message] (d/two-half-commit! this c/default-opts))
  (commit! [this _message opts] (d/two-half-commit! this opts))

  c/PConvergent
  (-join [this other] (c/-join this other c/default-opts))
  (-join [this other opts] (d/two-half-join this other opts))
  (-conflict-free? [_] true)

  c/PDeltaApply
  (-apply-delta [this delta] (apply-delta this delta))
  (-apply-delta [this delta opts] (apply-delta this delta opts))

  p/GarbageCollectable
  (gc-roots [this]
    (async+sync (:sync? c/default-opts) (async #{(await (p/snapshot-id this))})))
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids gc-opts] (d/two-half-gc-sweep! this snapshot-ids (merge c/default-opts (t/async-gc-opts "ormap/gc-sweep!" gc-opts))))

  p/Overlayable
  (overlay [this opts]
    (let [mode (or (:mode opts) :frozen)
          lw   (if (= :following mode)
                 (clojure.core/assoc this :adds (d/empty-set storage comparator)
                                     :removals (d/empty-set storage comparator) :dirty false)
                 (clojure.core/assoc this :dirty false))]
      (ovl/convergent-overlay this mode lw)))

  p/Graphable
  ;; commit-DAG methods all delegate to the shared `d/commit-*` helpers.
  (history [this] (p/history this c/default-opts))
  (history [_ opts] (d/commit-history kv-store storage config commit opts))
  (ancestors [this s] (p/ancestors this s c/default-opts))
  (ancestors [_ s opts] (d/commit-ancestors kv-store storage config s opts))
  (ancestor? [this a b] (p/ancestor? this a b c/default-opts))
  (ancestor? [_ a b opts] (d/commit-ancestor? kv-store storage config a b opts))
  (common-ancestor [this a b] (p/common-ancestor this a b c/default-opts))
  (common-ancestor [_ a b opts] (d/commit-common-ancestor kv-store storage config a b opts))
  (commit-graph [this] (p/commit-graph this c/default-opts))
  (commit-graph [_ opts] (d/commit-graph-map kv-store storage config opts))
  (commit-info [this id] (p/commit-info this id c/default-opts))
  (commit-info [_ id opts] (d/commit-info kv-store storage config id opts)))

;; ============================================================
;; Value ops — each returns a NEW record (value-semantic)
;; ============================================================
;; Each op takes an OPTIONAL trailing `opts` ({:sync?}); omit it for the
;; platform default (`c/default-opts`).

(defn assoc
  "Assoc `v` under `k` with a FRESH uid (local op). (async+sync) NEW o."
  ([o k v] (assoc o k v c/default-opts))
  ([o k v opts]
   (async+sync (:sync? opts)
               (async
                (let [entry [(hk k) (str (random-uuid)) k v]
                      s'    (await (d/set-conj (:adds o) entry (:comparator o) opts))
                      o'    (c/with-delta (clojure.core/assoc o :adds s' :dirty true)
                              half-accrue {:adds #{entry}})]
                  (if (:flush? opts true) (await (flush! o' opts)) o'))))))

(defn dissoc
  "Observed-remove `k`: tombstone exactly the entries currently live for it
   (slice-scoped, add-wins). (async+sync) NEW o."
  ([o k] (dissoc o k c/default-opts))
  ([o k opts]
   (async+sync (:sync? opts)
               (async
                (let [tombstones (await (live-for-key o k opts))
                      removals'  (loop [r (:removals o) ts (seq tombstones)]
                                   (if ts
                                     (recur (await (d/set-conj r (first ts) (:comparator o) opts)) (next ts))
                                     r))]
                  (let [o' (c/with-delta (clojure.core/assoc o :removals removals' :dirty true)
                             half-accrue {:removals (set tombstones)})]
                    (if (:flush? opts true) (await (flush! o' opts)) o')))))))

(defn apply-delta
  "Consume a peer's δ ({:adds #{entries} :removals #{entries}}) — union each half
   into the local halves (OP-path; cf. -join the STATE-path). NEW o. (async+sync)"
  ([o delta] (apply-delta o delta c/default-opts))
  ([o delta opts]
   (async+sync (:sync? opts)
               (async
                (let [adds (loop [a (:adds o) ps (seq (:adds delta))]
                             (if ps (recur (await (d/set-conj a (first ps) (:comparator o) opts)) (next ps)) a))
                      rems (loop [r (:removals o) ps (seq (:removals delta))]
                             (if ps (recur (await (d/set-conj r (first ps) (:comparator o) opts)) (next ps)) r))]
                  (let [o' (c/clear-delta (clojure.core/assoc o :adds adds :removals rems :dirty true))]
                    ;; AUTO-FLUSH the receive side — durable-after-apply.
                    (if (:flush? opts true) (await (flush! o' opts)) o')))))))

(defn get
  "The value for `k`: a SET of live values (plain) or the merge-fn FOLD of them
   (merging), or nil. A range SLICE — reads only `k`'s nodes. (async+sync)"
  ([o k] (get o k c/default-opts))
  ([o k opts]
   (async+sync (:sync? opts)
               (async
                (let [vs (map #(nth % 3) (await (live-for-key o k opts)))]
                  (when (seq vs)
                    (if-let [mfn (:merge-fn o)]
                      (reduce mfn nil (sort-by hash vs))
                      (set vs))))))))

(defn keys
  "The set of keys with ≥1 live value (full scan — inherently O(n)). (async+sync)"
  ([o] (keys o c/default-opts))
  ([o opts]
   (async+sync (:sync? opts)
               (async (into #{} (map #(nth % 2)) (await (all-live o opts)))))))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist both halves + :crdt/roots + freed; clear `dirty`. (async+sync)"
  ([o] (d/two-half-flush! o c/default-opts))
  ([o opts] (d/two-half-flush! o opts)))

(defn merge-peer!
  "Cross-store -join: ship the peer's nodes into this store and union both halves.
   Returns a NEW o. (async+sync)"
  ([o other] (merge-peer! o other c/default-opts))
  ([o other opts] (d/two-half-merge-peer! o other opts)))

(defn gc!
  ([o] (p/gc-sweep! o nil nil))
  ([o opts] (p/gc-sweep! o nil opts)))

(defn merge-base
  "The most recent common ancestor commit of branches `a` and `b` (git merge-base). (async+sync)"
  ([o a b] (merge-base o a b c/default-opts))
  ([o a b opts] (d/commit-merge-base (:kv-store o) (:storage o) (:config o) a b opts)))

;; ============================================================
;; Factories
;; ============================================================

(defn- open-ormap
  ;; `merge-fn` is a FUNCTION (not serializable) OR a registered ID (keyword,
  ;; `fn-registry/register-fn!`) → the fold survives a round-trip (stored as
  ;; `:merge-fn-id` in config; resolved on read). nil = plain multi-value OR-Map.
  [id merge-fn {:keys [store-config comparator kv-store cell-ns branches-key branch]
                :or {comparator compare branch :main}}
   {:keys [sync?] :or {sync? true}}]
  (let [open-opts   {:sync? sync?}
        merge-fn-id (when (keyword? merge-fn) merge-fn)
        mfn         (if (keyword? merge-fn) (fr/resolve-fn merge-fn) merge-fn)]
    (async+sync sync?
                (async
                 (let [{:keys [kv-store storage store-config config adds removals branch commit]}
                       (await (d/two-half-open store-config
                                               {:comparator comparator :kv-store kv-store
                                                :cell-ns cell-ns :branches-key branches-key :branch branch}
                                               open-opts))
                       config (cond-> config merge-fn-id (clojure.core/assoc :merge-fn-id merge-fn-id))]
                   (->ORMap id kv-store store-config storage comparator (wrap mfn)
                            adds removals false branch commit config))))))

(defn ormap
  "Open (or create) a durable OR-Map (multi-value: `get` returns the live value-set)."
  ([id] (open-ormap id nil {} {:sync? true}))
  ([id config] (open-ormap id nil config {:sync? true}))
  ([id config opts] (open-ormap id nil config opts)))

(defn merging-ormap
  "Open (or create) a durable Merging-OR-Map: concurrent per-key values FOLD via
   `merge-fn` (commutative/associative/idempotent) to a single value on `get`."
  ([id merge-fn] (open-ormap id merge-fn {} {:sync? true}))
  ([id merge-fn config] (open-ormap id merge-fn config {:sync? true}))
  ([id merge-fn config opts] (open-ormap id merge-fn config opts)))

;; Register the (plain) OR-Map with the system value codec (JVM). Both [hk uid k v]
;; halves ride as content addresses. merge-fn = nil → the multi-value (value-set)
;; OR-Map (system-type :ormap). A custom merging-ormap fold-fn is NOT serializable
;; here: the fold rides as `:merge-fn-id` in config (a registered id), resolved on
;; read. ONE project/reconstruct serves BOTH stypes — `:ormap` (no id → merge-fn nil
;; → multi-value) and `:merging-ormap` (id → the resolved fold). The entries
;; (adds/removals) are preserved regardless; only the read-time fold is recovered.
(let [project     (fn [{:keys [id store-config adds removals dirty branch commit config]}]
                    {:id id :store-config store-config
                     :adds     (d/root-node-blob adds)
                     :removals (d/root-node-blob removals)
                     :dirty dirty :branch branch :commit commit :config config})
      reconstruct (fn [blob storage opts]
                    (->ORMap (:id blob) (:kv-store storage) (:store-config blob) storage compare
                             (wrap (fr/resolve-fn (:merge-fn-id (:config blob))))   ; nil id → plain
                             (d/restore-fused compare (:adds blob) storage opts)
                             (d/restore-fused compare (:removals blob) storage opts)
                             (or (:dirty blob) false) (:branch blob) (:commit blob) (:config blob)))]
  (yf/register-system! :ormap ORMap project reconstruct)
  (yf/register-system! :merging-ormap ORMap project reconstruct))
