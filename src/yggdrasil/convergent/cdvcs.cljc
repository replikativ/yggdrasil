(ns yggdrasil.convergent.cdvcs
  "CDVCS as a DURABLE convergent yggdrasil system — the catalog's missing middle:
   datahike (single-head, 3-way merge) ↔ CDVCS (multi-head, conflict LIFTED into
   the head set, convergent metadata) ↔ flat CRDTs (no-conflict join).

   The convergent VALUE is the small metadata map
     {:commit-graph {id [parent-id ...]} :heads #{id} :version n}
   held in the record (value-semantic: every verb returns a NEW record). It lives
   durably in ONE cell, written CONVERGENTLY via `graph/downstream` so a shared-
   store peer's concurrent state JOINS rather than LWW-clobbers. The commits
   themselves are CONTENT-ADDRESSED blobs (`id` = hash of {:transactions :parents}),
   stored under their id key — so a commit id IS its address, additions are
   idempotent, and cross-peer sync is `ship!`-of-the-missing-blobs (konserve-sync),
   NOT kabel `-missing-commits`.

   `-join` ≡ replikativ `downstream`: merge the commit-graphs, recompute heads via
   the LCA cut — commutative/associative/idempotent on the grow-only DAG. It always
   converges and may leave >1 head: that IS the lifted conflict. `merge` (a verb,
   authored) resolves heads into one via a merge commit. So `-conflict-free?` is
   FALSE — CDVCS is the convergent system that tolerates conflict in its value.

   The pure heart is `cdvcs.graph` (DAG algebra) + `cdvcs.core` (value verbs); this
   ns is the durable + cross-platform (`async+sync`) wrapping. Git-like verbs
   (commit/merge/pull) — NOT collection verbs."
  (:refer-clojure :exclude [merge])
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.cdvcs.graph :as graph]
            [yggdrasil.convergent.cdvcs.core :as core]
            [yggdrasil.kbridge :as kb]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->CDVCS flush! cdvcs)

(def ^:private state-cell :cdvcs/state)
(defn- sk [opts] (:state-key opts state-cell))

(defn- state-of
  "The convergent state map of `x` — `x` itself if it is a bare state map (has
   `:commit-graph`), else `(:state x)` for a CDVCS record. Avoids an
   `instance?` against the not-yet-defined record class in the verbs below."
  [x]
  (if (:commit-graph x) x (:state x)))

;; ============================================================
;; Durable persistence — commits (content-addressed) + the state cell (convergent)
;; ============================================================

(defn- persist-commits!
  "k-assoc every {id -> commit-value} blob under its id (content-addressed,
   idempotent). (async+sync)"
  [kv-store commits opts]
  (async+sync (:sync? opts)
              (async
               (loop [cs (seq commits)]
                 (if cs
                   (let [[id cval] (first cs)]
                     (await (kb/k-assoc kv-store id cval opts))
                     (recur (next cs)))
                   true)))))

(defn- save-state!
  "Write the convergent state map into its cell, JOINED with whatever is already
   there via `graph/downstream` — so a concurrent shared-store peer's state never
   gets clobbered (TOCTOU-safe: a single atomic konserve `update`). (async+sync)"
  [kv-store state opts]
  (async+sync (:sync? opts)
              (async
               (await (kb/k-update kv-store (sk opts)
                                   (fn [existing]
                                     (if existing (graph/downstream existing state) state))
                                   opts)))))

;; ============================================================
;; Functional verbs — each returns a NEW record (value-semantic)
;; ============================================================

(defn- with-step
  "Apply a core verb result {:state :commits}: persist the new commit blobs
   eagerly (immutable, cheap) and return a NEW dirty record carrying the new
   state. The state cell is written on `flush!`. (async+sync)"
  [cd {:keys [state commits]}]
  (async+sync (:sync? (:opts cd))
              (async
               (await (persist-commits! (:kv-store cd) commits (:opts cd)))
               (->CDVCS (:id cd) (:kv-store cd) (:store-config cd)
                        state true (:opts cd)))))

(defn commit
  "Commit `transactions` onto a single-head CDVCS (throws on multiple heads).
   Returns a NEW record. (async+sync)"
  [cd author transactions]
  (with-step cd (core/commit (:state cd) author transactions)))

(defn merge
  "Reconcile this CDVCS with `remote` (another CDVCS or a bare state map),
   or its OWN multiple heads (pass it itself): join the graphs + record a merge
   commit. Returns a NEW single-head record. (async+sync)"
  ([cd author remote] (merge cd author remote []))
  ([cd author remote correcting-transactions]
   (let [remote-state (state-of remote)
         heads        (core/merge-heads (:state cd) remote-state)]
     (with-step cd (core/merge (:state cd) author remote-state heads correcting-transactions)))))

(defn pull
  "Fast-forward to `remote-tip` from `remote`'s graph (throws if it would induce a
   conflict — use `merge`). Returns a NEW record. (async+sync)"
  [cd remote remote-tip]
  (with-step cd (core/pull (:state cd) (state-of remote) remote-tip)))

(defn heads        [cd] (:heads (:state cd)))
(defn commit-graph [cd] (:commit-graph (:state cd)))
(defn multiple-heads? [cd] (core/multiple-heads? (:state cd)))

(defn history
  "The linear commit history (ids) of a single head — DFS linearisation. Throws
   on multiple heads (pick one explicitly via `(graph/commit-history g head)`)."
  [cd]
  (if (multiple-heads? cd)
    (throw (ex-info "CDVCS has multiple heads — linearise a chosen head explicitly."
                    {:type :multiple-heads :heads (heads cd)}))
    (graph/commit-history (commit-graph cd) (first (heads cd)))))

(defn read-commit
  "Read a commit blob by its id. (async+sync)"
  [cd id]
  (d/read-commit (:kv-store cd) id (:opts cd)))

(defn ship!
  "Copy every commit blob in `cd`'s commit-graph that `dst-store` is MISSING into
   it (content-addressed by id, so this is exactly what konserve-sync ships — no
   PSS reachability walk; the commit ids ARE the addresses). Incremental +
   idempotent; returns the count copied. The store-to-store transport that lets a
   cross-store `-join` see the peer's commits. (async+sync)"
  [cd dst-store]
  (let [opts (:opts cd)
        src  (:kv-store cd)]
    (async+sync (:sync? opts)
                (async
                 (loop [ids (seq (keys (:commit-graph (:state cd)))) n 0]
                   (if ids
                     (let [id (first ids)]
                       (if (some? (await (kb/k-get dst-store id opts)))
                         (recur (next ids) n)
                         (do (await (kb/k-assoc dst-store id (await (kb/k-get src id opts)) opts))
                             (recur (next ids) (inc n)))))
                     n))))))

;; ============================================================
;; Record
;; ============================================================

(defrecord CDVCS
           [id kv-store store-config
            state     ; {:commit-graph :heads :version} — the convergent value
            dirty     ; unsaved state cell?
            opts]     ; the record's sync-mode

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :cdvcs)
  ;; it is genuinely a versioned DAG (graphable) whose merge is an authored verb
  ;; resolving the lifted conflict; replicas sync as separate stores via konserve-
  ;; sync (so the protocol-level branch!/merge! hierarchical tier is not used).
  (capabilities [_] {:snapshotable true :branchable false :mergeable false
                     :garbage-collectable true :overlayable false
                     :graphable true})

  p/Snapshotable
  ;; addressable snapshot = the content-addressed STATE map (commit-graph + heads).
  ;; Equal value ⇒ equal snapshot-id. `as-of` restores the FROZEN state value
  ;; (realising a head's application value needs an app eval-fn — left to callers).
  (snapshot-id [_]
    (async+sync (:sync? opts)
                (async (str (await (d/store-commit! kv-store state opts))))))
  (parent-ids [_] #{})
  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (async+sync (:sync? opts)
                (async (await (d/read-commit kv-store (parse-uuid (str snap-id)) opts)))))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  p/Branchable
  (branches [_] #{:main}) (branches [_ _] #{:main})
  (current-branch [_] :main)
  (branch! [this _] this) (branch! [this _ _] this) (branch! [this _ _ _] this)
  (delete-branch! [this _] this) (delete-branch! [this _ _] this)
  (checkout [this _] this) (checkout [this _ _] this)

  p/Mergeable
  (merge! [this _] this) (merge! [this _ _] this)
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {})

  p/Committable
  (commit! [this] (flush! this))
  (commit! [this _message] (flush! this))
  (commit! [this _message _opts] (flush! this))

  c/PConvergent
  ;; -join ≡ downstream: merge commit-graphs + recompute heads. Always converges;
  ;; may leave >1 head (the lifted conflict). Commit blobs of `other` must already
  ;; be in this store (same store, or ship!ed first). (async+sync)
  (-join [this other]
    (async+sync (:sync? opts)
                (async
                 (let [other-state (state-of other)
                       joined      (graph/downstream state other-state)]
                   (if (= joined state)
                     this   ; idempotent no-op
                     (do (await (save-state! kv-store joined opts))
                         (->CDVCS id kv-store store-config joined false opts)))))))
  (-conflict-free? [_] false)   ; CDVCS LIFTS conflict into the head set

  p/GarbageCollectable
  (gc-roots [this]
    (async+sync (:sync? opts) (async #{(await (p/snapshot-id this))})))
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids gc-opts]
    (async+sync (:sync? opts)
                (async
                 (await (flush! this))
                 ;; reachable = the state cell + every LIVE commit id + (for each
                 ;; retained snapshot) its state-commit addr + that snapshot's whole
                 ;; commit-graph. Commits are content-addressed plain blobs (no PSS
                 ;; :addresses), so `roots`=[] and we spare them by KEY. The cutoff
                 ;; (`:remove-before`/`:grace-period-ms`) rides in `gc-opts`.
                 (let [snap-addrs (map #(parse-uuid (str %)) snapshot-ids)
                       retain     (loop [as (seq snap-addrs)
                                         acc (set (keys (:commit-graph state)))]
                                    (if as
                                      (let [snap (await (d/read-commit kv-store (first as) (:opts this)))]
                                        (recur (next as)
                                               (into (conj acc (first as))
                                                     (keys (:commit-graph snap)))))
                                      acc))]
                   (await (d/gc! kv-store []
                                 (clojure.core/merge gc-opts opts
                                                     {:state-key (sk opts)
                                                      :spare-keys [(sk opts)]
                                                      :retain-keys (vec retain)}))))))))

(defn flush!
  "Persist the convergent state into its cell (convergent write). Commit blobs are
   already durable (written eagerly per verb). Returns a clean record. (async+sync)"
  [cd]
  (async+sync (:sync? (:opts cd))
              (async
               (await (save-state! (:kv-store cd) (:state cd) (:opts cd)))
               (assoc cd :dirty false))))

;; ============================================================
;; Factory
;; ============================================================

(defn cdvcs
  "Open (or create) a durable CDVCS. Loads the state cell if present; otherwise
   seeds a fresh single-base-commit CDVCS for `:author` and persists it. Returns
   (async+sync) a CDVCS.

   opts: :store-config (konserve cfg) | :kv-store (pre-opened, for a shared store),
         :author, :sync? (default true), :state-key (cell key, default :cdvcs/state)."
  [id & {:keys [author sync?] :or {sync? true} :as opts}]
  (let [opts (clojure.core/merge {:sync? sync?} (dissoc opts :author))]
    (async+sync sync?
                (async
                 (let [{:keys [kv-store store-config]} (await (d/open (:store-config opts) opts))
                       existing (await (kb/k-get kv-store (sk opts) opts))]
                   (if existing
                     (->CDVCS id kv-store store-config existing false opts)
                     (let [{:keys [state commits]} (core/new-cdvcs author)]
                       (await (persist-commits! kv-store commits opts))
                       (await (save-state! kv-store state opts))
                       (->CDVCS id kv-store store-config state false opts))))))))
