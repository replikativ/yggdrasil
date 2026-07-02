(ns yggdrasil.convergent.twopset
  "Two-Phase Set (2P-Set) as a DURABLE conflict-free yggdrasil system.

   TWO grow-only, content-addressed PSS sets of BARE elements —
     adds      every added element        (root under :crdt/roots :adds)
     removals  tombstones for removed els  (… :crdt/roots :removals)
   with live(e) ⇔ e ∈ adds ∧ e ∉ removals. Convergent add+remove where REMOVE
   is permanent: re-adding a removed element keeps it removed (the content-add
   re-enters `adds`, but it is also in `removals`). For re-add-after-remove use
   the OR-Set (`orset`).

   Unlike the OR-Set this stores BARE elements (no `[elem tag]` pairs), so an
   element fressian handler (`:element-read-handlers`/`:element-write-handlers` —
   e.g. the registry's RegistryEntry) applies cleanly and there is no doubled
   storage. Elements may be any fressian-native value or, with a handler, records;
   a `:comparator` orders them (default `compare`).

   VALUE SEMANTICS: the record holds the two halves (`adds`/`removals`, immutable
   PSS-set values) and a `dirty` flag as PLAIN fields; every mutator returns a NEW
   record (never mutates in place). The mutable cell lives in the HOLDER — the
   registry conn or a spindel signal-atom — which swaps this value.

   Implements SystemIdentity / Snapshotable / Overlayable / PConvergent — so a
   2P-Set (and the registry, a lens over one) is a first-class yggdrasil system
   that another yggdrasil can itself track, fork, and merge. Content-addressing
   gives it a stable snapshot identity. **Fully cross-platform**: the record does
   NOT carry an execution mode — each content-touching op takes an OPTIONAL trailing
   `opts` ({:sync?}, default `c/default-opts`)."
  (:refer-clojure :exclude [conj disj contains? into])
  (:require [clojure.set :as set]
            [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.overlay :as ovl]
            [yggdrasil.fressian :as yf]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->TwoPSet flush! apply-delta)

(defn- half-accrue
  "Accrue δ half-maps {:adds #{..} :removals #{..}} by unioning each half."
  [a b]
  (merge-with clojure.core/into a b))

(defrecord TwoPSet
           [id kv-store store-config storage comparator
            adds         ; CURRENT branch's immutable PSS set of elements
            removals     ; CURRENT branch's immutable PSS set of tombstoned elements
            dirty        ; boolean — content changed since last flush
            branch       ; current branch keyword
            commit       ; branch TIP commit-id (string) in the commit-DAG; nil before base seed
            config]      ; DOMAIN: cell-keys (:branches-key/:cell-ns); {} ⇒ store defaults

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :2p-set)
  ;; genuinely branchable (per-branch head cells; shared two-half machinery).
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true :overlayable true
                     :graphable true})

  p/Snapshotable
  ;; addressable snapshot = a content-addressed COMMIT {:adds :removals} over the
  ;; two halves' roots (shared two-half machinery); `as-of` projects the FROZEN
  ;; value (adds − removals).
  (snapshot-id [this] (d/two-half-snapshot-id this c/default-opts))
  (parent-ids [_] (if commit #{commit} #{}))
  (as-of [this snap-id] (p/as-of this snap-id c/default-opts))
  (as-of [this snap-id opts]
    (async+sync (:sync? opts)
                (async
                 (let [[a r] (await (d/two-half-restore-halves this snap-id opts))]
                   (set/difference a r)))))
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
  ;; PURE same-store join: union both halves with the peer (shared). (async+sync)
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
  (gc-sweep! [this snapshot-ids gc-opts] (d/two-half-gc-sweep! this snapshot-ids (merge c/default-opts (t/async-gc-opts "twopset/gc-sweep!" gc-opts))))

  p/Overlayable
  ;; :frozen → carry BOTH halves (immutable PSS values; isolated by value).
  ;; :following → empty delta halves; `ovl/overlay-value` joins with the LIVE
  ;; parent on read. Mutate the clone via `ovl/overlay-swap!`.
  (overlay [this opts]
    (let [mode (or (:mode opts) :frozen)
          lw   (if (= :following mode)
                 (assoc this :adds (d/empty-set storage comparator)
                        :removals (d/empty-set storage comparator) :dirty false)
                 (assoc this :dirty false))]
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
;; Value ops — each returns a NEW 2P-Set value (value-semantic)
;; ============================================================
;; Each op takes an OPTIONAL trailing `opts` ({:sync?}); omit it for the
;; platform default (`c/default-opts`).

(defn- conj-into!
  "async+sync: conj `elem` onto the PSS set in field `field` (:adds or :removals)
   of `s`, mark dirty, and record the op as a local δ. Returns a NEW s."
  [s field elem opts]
  (async+sync (:sync? opts)
              (async
               (let [s'  (await (d/set-conj (get s field) elem (:comparator s) opts))
                     s'' (c/with-delta (assoc s field s' :dirty true)
                           half-accrue
                           (if (= field :adds) {:adds #{elem}} {:removals #{elem}}))]
                 ;; AUTO-FLUSH (default true; `into` passes `{:flush? false}` to batch).
                 (if (:flush? opts true) (await (flush! s'' opts)) s'')))))

(defn conj
  "Add `elem`. (async+sync) Returns a NEW s."
  ([s elem] (conj s elem c/default-opts))
  ([s elem opts] (conj-into! s :adds elem opts)))

(defn into
  "Add many elements. (async+sync) Returns a NEW s."
  ([s elems] (into s elems c/default-opts))
  ([s elems opts]
   (async+sync (:sync? opts)
               (async
                ;; batch: conj each element WITHOUT flushing, then flush once at the end.
                (let [batched (loop [s s es (seq elems)]
                                (if es
                                  (recur (await (conj-into! s :adds (first es) (assoc opts :flush? false))) (next es))
                                  s))]
                  (if (:flush? opts true) (await (flush! batched opts)) batched))))))

(defn disj
  "Tombstone `elem` (permanent — 2P-Set semantics). (async+sync) Returns a NEW s."
  ([s elem] (disj s elem c/default-opts))
  ([s elem opts] (conj-into! s :removals elem opts)))

(defn apply-delta
  "Consume a peer's 2P-Set δ ({:adds #{..} :removals #{..}}) by unioning each half
   into the local halves — the OP-path apply (O(δ); cf. -join the STATE-path).
   Returns a NEW s. (async+sync)"
  ([s delta] (apply-delta s delta c/default-opts))
  ([s delta opts]
   (async+sync (:sync? opts)
               (async
                (let [adds (loop [a (:adds s) es (seq (:adds delta))]
                             (if es (recur (await (d/set-conj a (first es) (:comparator s) opts)) (next es)) a))
                      rems (loop [r (:removals s) es (seq (:removals delta))]
                             (if es (recur (await (d/set-conj r (first es) (:comparator s) opts)) (next es)) r))]
                  ;; clear δ: a remote-integrated value re-propagates nothing.
                  (let [s' (c/clear-delta (assoc s :adds adds :removals rems :dirty true))]
                    ;; AUTO-FLUSH the receive side — durable-after-apply.
                    (if (:flush? opts true) (await (flush! s' opts)) s')))))))

(defn elements
  "Live elements: adds − removals. (async+sync)"
  ([s] (elements s c/default-opts))
  ([s opts]
   (async+sync (:sync? opts)
               (async (set/difference (await (d/set->clj (:adds s) opts))
                                      (await (d/set->clj (:removals s) opts)))))))

(defn contains?
  "Whether `elem` is live. (async+sync)"
  ([s elem] (contains? s elem c/default-opts))
  ([s elem opts]
   (async+sync (:sync? opts)
               (async (clojure.core/contains? (await (elements s opts)) elem)))))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist both halves + the :crdt/roots cell + freed; clear `dirty`. (async+sync)"
  ([s] (d/two-half-flush! s c/default-opts))
  ([s opts] (d/two-half-flush! s opts)))

(defn merge-peer!
  "Cross-store -join: ship the peer's nodes here and union both halves. (async+sync)"
  ([s other] (merge-peer! s other c/default-opts))
  ([s other opts] (d/two-half-merge-peer! s other opts)))

(defn gc!
  "Reclaim PSS nodes unreachable from the live adds/removals roots (mark-and-sweep).
   Returns the set of deleted node keys."
  ([s] (p/gc-sweep! s nil nil))
  ([s opts] (p/gc-sweep! s nil opts)))

(defn merge-base
  "The most recent common ancestor commit of branches `a` and `b` (git merge-base). (async+sync)"
  ([s a b] (merge-base s a b c/default-opts))
  ([s a b opts] (d/commit-merge-base (:kv-store s) (:storage s) (:config s) a b opts)))

;; ============================================================
;; Factory
;; ============================================================

(defn twopset
  "Open (or create) a durable 2P-Set on a per-system konserve store.

   :comparator             element order (default compare)
   :element-read-handlers/:element-write-handlers
                            fressian handlers for the element type (default none —
                            elements are fressian-native; the registry passes a
                            RegistryEntry handler)
   Restores both halves from the store's :crdt/roots cell when present. The runtime
   mode is a CONSTRUCTION-time choice for opening the store; it is NOT stamped on the
   record — each op picks its own `:sync?` (default `c/default-opts`)."
  ([id] (twopset id {} {:sync? true}))
  ([id config] (twopset id config {:sync? true}))
  ([id {:keys [comparator element-read-handlers element-write-handlers store-config kv-store cell-ns branches-key branch]
        :or {comparator compare branch :main}}
    {:keys [sync?] :or {sync? true}}]
   (let [open-opts {:sync? sync?}]
     (async+sync sync?
                 (async
                  (let [{:keys [kv-store storage store-config config adds removals branch commit]}
                        (await (d/two-half-open store-config
                                                {:comparator comparator :kv-store kv-store
                                                 :cell-ns cell-ns :branches-key branches-key :branch branch
                                                 :element-read-handlers element-read-handlers
                                                 :element-write-handlers element-write-handlers}
                                                open-opts))]
                    (->TwoPSet id kv-store store-config storage comparator adds removals false branch commit config)))))))

;; Register the 2P-Set with the system value codec (JVM). Both halves ride as their
;; content-addressed root addresses; `compare` is the right comparator (and what
;; slice would read). storage/kv-store re-derived on read.
(yf/register-system!
 :2p-set TwoPSet
 ;; project reads the ALREADY-COMMITTED roots (assert-committed; auto-flush guarantees it).
 (fn [{:keys [id store-config adds removals dirty branch commit config]}]
   {:id id :store-config store-config
    :adds     (d/root-node-blob adds)
    :removals (d/root-node-blob removals)
    :dirty dirty :branch branch :commit commit :config config})
 (fn [blob storage opts]
   (->TwoPSet (:id blob) (:kv-store storage) (:store-config blob) storage compare
              (d/restore-fused compare (:adds blob) storage opts)
              (d/restore-fused compare (:removals blob) storage opts)
              (or (:dirty blob) false) (:branch blob) (:commit blob) (:config blob))))
