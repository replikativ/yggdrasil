(ns yggdrasil.convergent.cdvcs
  "CDVCS as a DURABLE convergent yggdrasil system — the catalog's missing middle:
   datahike (single-head, 3-way merge) ↔ CDVCS (multi-head, conflict LIFTED into
   the head set, convergent metadata) ↔ flat CRDTs (no-conflict join).

   STORAGE (Option B — aligned with the PSS CRDTs, BOUNDED resident heap): the
   commit-graph is a grow-only **PSS** of `[id parents]` entries (the convergent
   source of truth — its roots cell merges as a grow-map, so mutually-merged peers
   converge on the same content-addressed root, exactly like a G-Set). A commit's
   parents are read ON THE FLY (`parents-of` slices the entry) by the store-backed
   algebra in `cdvcs.graph-store`; the full graph is never resident. `:heads` (the
   DERIVED frontier, `#{id}`) + `:version` live in a small cache cell — heads can't
   be convergent (a commit removes its parents from heads), so `-join` recomputes
   them via `remove-ancestors` over the merged graph. A peer's heads reflect its
   last `-join` (fresh under signal-sync; a cold-opened peer converges on its next
   `-join`). The commits THEMSELVES stay content-addressed blobs (`id` = hash of
   `{:transactions :parents}`), idempotent, shipped by konserve-sync.

   `-join` ≡ replikativ `downstream`: union the commit-graphs + recompute heads —
   commutative/associative/idempotent. May leave >1 head: the lifted conflict.
   `merge` (authored) resolves heads via a merge commit. `-conflict-free?` is FALSE.

   The pure `cdvcs.graph` + `cdvcs.core` stay as the value-level reference (covered
   by the pure `cdvcs_test`); this ns is the durable + cross-platform wrapping."
  (:refer-clojure :exclude [merge])
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.cdvcs.core :as core]
            [yggdrasil.convergent.cdvcs.graph-store :as gs]
            [yggdrasil.kbridge :as kb]
            [clojure.set :as set]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->CDVCS flush! cdvcs parents-of apply-delta)

;; cells (DOMAIN — overridable via `config`)
(def ^:private graph-cell :cdvcs/graph)
(def ^:private graph-freed-cell :cdvcs/graph-freed)
(def ^:private state-cell :cdvcs/state)
(defn- gk  [config] (:graph-key config graph-cell))
(defn- gfk [config] (:graph-freed-key config graph-freed-cell))
(defn- sk  [config] (:state-key config state-cell))
(defn- graph-config [config] {:roots-key (gk config) :freed-key (gfk config)})

;; commit-graph entries `[id parents]`, ordered by id only — so a partial `[id]`
;; vector is a valid slice bound (the comparator ignores the rest).
(def ^:private graph-cmp (fn [a b] (compare (first a) (first b))))

;; ============================================================
;; Graph PSS + commit-blob persistence
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

(defn- load-graph
  "Restore the commit-graph PSS from its roots cell (empty when none). (async+sync)"
  [kv-store storage config opts]
  (async+sync (:sync? opts)
              (async
               (let [root (:main (await (d/load-roots kv-store (graph-config config) opts)))]
                 (if root
                   (d/restore-set graph-cmp root storage opts)
                   (d/empty-set storage graph-cmp))))))

(defn- save-graph!
  "Persist the graph PSS (nodes + roots cell + freed), returning its root. The roots
   cell is a CONVERGENT grow-map write (`save-roots!`). (async+sync)"
  [kv-store graph storage config opts]
  (async+sync (:sync? opts)
              (async
               (let [root (await (d/store-set! graph storage opts))]
                 (await (d/save-roots! kv-store {:main root} (graph-config config) opts))
                 (await (d/save-freed! kv-store storage (graph-config config) opts))
                 root))))

(defn- save-state!
  "Write the {:heads :version} CACHE cell (LWW — the graph PSS is the convergent
   truth; `-join` refreshes heads). (async+sync)"
  [kv-store state config opts]
  (async+sync (:sync? opts)
              (async (await (kb/k-assoc kv-store (sk config) state opts)))))

(defn parents-of
  "An accessor `id -> (async parents-or-nil)` over a graph PSS `graph` (read through
   `storage`), for the store-backed algebra: slices the `[id parents]` entry and
   yields its parents, or nil when the commit is absent. (async+sync)"
  [graph storage opts]
  (fn [id]
    (async+sync (:sync? opts)
                (async
                 (let [es (await (d/slice->clj graph [id] [id] opts))]
                   (when (seq es) (second (first es))))))))

;; ============================================================
;; Functional verbs — each returns a NEW record (value-semantic)
;; ============================================================

(defn commit
  "Commit `transactions` onto a single-head CDVCS (throws on multiple heads). Appends
   `[id parents]` to the graph PSS + the content-addressed blob. (async+sync)"
  [cd author transactions]
  (let [opts (:opts cd)]
    (async+sync (:sync? opts)
                (async
                 (let [heads (:heads (:state cd))]
                   (when (not= 1 (count heads))
                     (throw (ex-info "CDVCS has multiple heads — merge before committing."
                                     {:type :multiple-heads :heads heads})))
                   (let [{:keys [id value]} (core/make-commit author (vec heads) transactions)
                         graph' (await (d/set-conj (:graph cd) [id (vec heads)] graph-cmp opts))]
                     (await (persist-commits! (:kv-store cd) {id value} opts))
                     ;; accrue the OP δ — the FULL self-contained commit {:id :value}
                     ;; (the blob carries :parents), so a peer applies it with no blob
                     ;; fetch (content-addressing dedups a re-stored blob).
                     (c/with-delta (assoc cd :graph graph'
                                          :state {:heads #{id} :version (inc (:version (:state cd)))}
                                          :dirty true)
                       set/union #{{:id id :value value}})))))))

(defn merge
  "Reconcile this CDVCS with `remote` (a CDVCS record), or its OWN multiple heads
   (pass it itself): UNION the graphs (set-union subsumes replikativ's `visited-b`
   selection) + record a merge commit whose parents are all the heads. Returns a NEW
   single-head record. (async+sync)"
  ([cd author remote] (merge cd author remote []))
  ([cd author remote correcting-transactions]
   (let [opts (:opts cd)]
     (async+sync (:sync? opts)
                 (async
                  (let [merged    (await (d/set-union (:graph cd) (:graph remote) graph-cmp opts))
                        all-heads (vec (set/union (:heads (:state cd)) (:heads (:state remote))))
                        {:keys [id value]} (core/make-commit author all-heads correcting-transactions)
                        graph'    (await (d/set-conj merged [id all-heads] graph-cmp opts))]
                    (await (persist-commits! (:kv-store cd) {id value} opts))
                    ;; the δ carries BOTH the merge commit AND the remote's commits we
                    ;; just unioned in (so a peer that has neither converges from the δ
                    ;; alone); the remote's δ — if any — is folded in too.
                    (let [remote-delta (or (c/delta-of remote) #{})]
                      (c/with-delta (assoc cd :graph graph'
                                           :state {:heads #{id}
                                                   :version (inc (max (:version (:state cd)) (:version (:state remote))))}
                                           :dirty true)
                        set/union (set/union remote-delta #{{:id id :value value}})))))))))

(defn pull
  "Fast-forward to `remote-tip` from `remote`'s graph. Throws if the remote is not a
   superset (use `merge`) or if pulling would induce multiple heads. (async+sync)"
  [cd remote remote-tip]
  (let [opts (:opts cd)]
    (async+sync (:sync? opts)
                (async
                 (when (some? (await ((parents-of (:graph cd) (:storage cd) opts) remote-tip)))
                   (throw (ex-info "No pull necessary — remote-tip already present."
                                   {:type :pull-unnecessary :remote-tip remote-tip})))
                 (let [cd-heads (:heads (:state cd))
                       merged   (await (d/set-union (:graph cd) (:graph remote) graph-cmp opts))
                       pof-cd   (parents-of (:graph cd) (:storage cd) opts)
                       pof-rem  (parents-of (:graph remote) (:storage remote) opts)
                       pof-m    (parents-of merged (:storage cd) opts)
                       {:keys [lcas]} (await (gs/lowest-common-ancestors
                                              pof-cd cd-heads pof-rem #{remote-tip} opts))
                       new-heads (await (gs/remove-ancestors pof-m pof-cd cd-heads #{remote-tip} opts))]
                   (when-not (set/superset? lcas cd-heads)
                     (throw (ex-info "Remote is not pullable (not a superset) — use merge."
                                     {:type :not-superset :lcas lcas :heads cd-heads})))
                   (when (> (count new-heads) 1)
                     (throw (ex-info "Cannot pull without inducing a conflict — use merge."
                                     {:type :multiple-heads :heads new-heads})))
                   (await (save-graph! (:kv-store cd) merged (:storage cd) (:config cd) opts))
                   (let [state' {:heads new-heads :version (inc (:version (:state cd)))}]
                     (await (save-state! (:kv-store cd) state' (:config cd) opts))
                     (assoc cd :graph merged :state state' :dirty false)))))))

(defn apply-delta
  "OP-path: integrate a peer's δ (a set of self-contained commits `{:id :value}`,
   the blob carrying `:parents`) into this CDVCS — persist each blob (content-
   addressed ⇒ idempotent), set-conj `[id parents]` into the graph, then recompute
   the frontier via `remove-ancestors` (≡ the STATE-path `-join`, but O(δ) not
   O(graph)). Returns a δ-FREE record (remote ops do not re-propagate). (async+sync)"
  [cd delta]
  (let [opts (:opts cd)]
    (async+sync (:sync? opts)
                (async
                 (let [entries (vec delta)]
                   (if (empty? entries)
                     (c/clear-delta cd)
                     (let [_      (await (persist-commits! (:kv-store cd)
                                                           (into {} (map (juxt :id :value)) entries) opts))
                           graph' (loop [es (seq entries) g (:graph cd)]
                                    (if es
                                      (let [{:keys [id value]} (first es)]
                                        (recur (next es)
                                               (await (d/set-conj g [id (vec (:parents value))] graph-cmp opts))))
                                      g))]
                       (if (= graph' (:graph cd))
                         (c/clear-delta cd)                      ; nothing new — idempotent
                         (let [all-parents (set (mapcat #(:parents (:value %)) entries))
                               ;; the δ's OWN frontier: incoming ids that are not a parent
                               ;; of another incoming commit — a valid head set, so the
                               ;; recompute matches `-join`'s (heads-a ∪ heads-b − LCAs).
                               incoming    (into #{} (comp (map :id) (remove all-parents)) entries)
                               new-heads   (await (gs/remove-ancestors
                                                   (parents-of graph' (:storage cd) opts)
                                                   (parents-of (:graph cd) (:storage cd) opts)
                                                   (:heads (:state cd)) incoming opts))]
                           (await (save-graph! (:kv-store cd) graph' (:storage cd) (:config cd) opts))
                           (let [state' {:heads new-heads :version (inc (:version (:state cd)))}]
                             (await (save-state! (:kv-store cd) state' (:config cd) opts))
                             (c/clear-delta (assoc cd :graph graph' :state state' :dirty false))))))))))))

(defn heads          [cd] (:heads (:state cd)))
(defn multiple-heads? [cd] (> (count (:heads (:state cd))) 1))

(defn commit-graph
  "Drain the graph PSS into a `{id -> parents}` map. O(graph) — debug/inspection
   only; the durable layer never materialises it. (async+sync)"
  [cd]
  (let [opts (:opts cd)]
    (async+sync (:sync? opts)
                (async
                 (into {} (map (fn [e] [(first e) (second e)]))
                       (await (d/set->clj (:graph cd) opts)))))))

(defn full-delta
  "The ENTIRE commit set as a δ — a set of self-contained `{:id :value}` commits
   (each blob carries `:parents`). The SERIALIZABLE full-state projection (plain
   data) for a connect handshake / catch-up: pass as signal-sync's `state-fn` so a
   joiner reconstructs the whole lineage via `-apply-delta`, instead of shipping the
   non-serializable CDVCS record. (async+sync) (sync mode only when used as a
   handshake state-fn — the handshake hashes the result synchronously.)"
  [cd]
  (let [opts (:opts cd)]
    (async+sync (:sync? opts)
                (async
                 (let [ids (map first (await (d/set->clj (:graph cd) opts)))]
                   (loop [is (seq ids) acc #{}]
                     (if is
                       (let [id (first is)
                             v  (await (d/read-commit (:kv-store cd) id opts))]
                         (recur (next is) (conj acc {:id id :value v})))
                       acc)))))))

(defn history
  "Linear commit history (ids) of the single head — DFS over the graph PSS. Throws on
   multiple heads. (async+sync) Delegates DIRECTLY to the (already async+sync)
   `gs/commit-history` — wrapping a pure tail-await in its own `async+sync` leaves the
   inner CPS fn unresolved on a nested await (cljs)."
  [cd]
  (let [heads (:heads (:state cd))]
    (when (> (count heads) 1)
      (throw (ex-info "CDVCS has multiple heads — linearise a chosen head explicitly."
                      {:type :multiple-heads :heads heads})))
    (gs/commit-history (parents-of (:graph cd) (:storage cd) (:opts cd)) (first heads) (:opts cd))))

(defn read-commit
  "Read a commit blob by its id. (async+sync)"
  [cd id]
  (d/read-commit (:kv-store cd) id (:opts cd)))

(defn ship!
  "Copy to `dst-store` every graph PSS node AND commit blob it is MISSING (content-
   addressed ⇒ incremental + idempotent). The store-to-store transport that lets a
   cross-store peer restore + `-join`. Returns the count copied. (async+sync)"
  [cd dst-store]
  (let [opts (:opts cd) src (:kv-store cd) storage (:storage cd)]
    (async+sync (:sync? opts)
                (async
                 (let [graph-root (await (d/store-set! (:graph cd) storage opts))
                       n1 (await (d/ship! src dst-store graph-root opts))
                       ids (map first (await (d/set->clj (:graph cd) opts)))
                       n2 (loop [is (seq ids) n 0]
                            (if is
                              (let [id (first is)]
                                (if (some? (await (kb/k-get dst-store id opts)))
                                  (recur (next is) n)
                                  (do (await (kb/k-assoc dst-store id (await (kb/k-get src id opts)) opts))
                                      (recur (next is) (inc n)))))
                              n))]
                   (+ n1 n2))))))

;; ============================================================
;; Record
;; ============================================================

(defrecord CDVCS
           [id kv-store store-config storage
            graph     ; grow-only PSS of [id parents] — the convergent commit-graph
            state     ; {:heads #{id} :version n} — the DERIVED frontier cache
            dirty     ; unflushed graph/state?
            config    ; DOMAIN: {:graph-key :state-key …}
            opts]     ; RUNTIME: {:sync?}

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :cdvcs)
  (capabilities [_] {:snapshotable true :branchable false :mergeable false
                     :garbage-collectable true :overlayable false
                     :graphable true})

  p/Snapshotable
  ;; addressable snapshot = a content-addressed {:graph <root> :heads :version} —
  ;; equal value ⇒ equal id. `as-of` returns that FROZEN handle (realise the graph
  ;; with `(restore-set graph-cmp (:graph snap) storage)`).
  (snapshot-id [_]
    (async+sync (:sync? opts)
                (async
                 (let [root (await (d/store-set! graph storage opts))]
                   (str (await (d/store-commit! kv-store {:graph root
                                                          :heads (:heads state)
                                                          :version (:version state)} opts)))))))
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
  ;; -join ≡ downstream: UNION the graph PSSs + recompute heads via remove-ancestors
  ;; over the merged graph. Always converges; may leave >1 head (the lifted conflict).
  ;; `other`'s graph nodes must be restorable here (same store, or ship!ed first).
  ;; IDEMPOTENCE: the graph union is grow-only, so `(= merged graph)` ⟺ `other` added
  ;; nothing ⟺ no-op — return `this` IDENTICAL (else a mutually-synced signal would
  ;; re-publish forever); equal graph also implies unchanged heads, so the head
  ;; recompute is skipped too. (Matches gset's `(= joined roots)`.) (async+sync)
  (-join [this other]
    (async+sync (:sync? opts)
                (async
                 (let [merged (await (d/set-union graph (:graph other) graph-cmp opts))]
                   (if (= merged graph)
                     this
                     (let [os        (:state other)
                           new-heads (await (gs/remove-ancestors
                                             (parents-of merged storage opts)
                                             (parents-of graph storage opts)
                                             (:heads state) (:heads os) opts))]
                       (await (save-graph! kv-store merged storage config opts))
                       (let [state' {:heads new-heads :version (max (:version state) (:version os))}]
                         (await (save-state! kv-store state' config opts))
                         (assoc this :graph merged :state state' :dirty false))))))))
  (-conflict-free? [_] false)   ; CDVCS LIFTS conflict into the head set

  c/PDeltaApply
  ;; OP-path counterpart to -join: consume a peer's δ of full commits. Lets signal-
  ;; sync carry CDVCS over the wire (δ self-contained ⇒ no blob fetch) — the SAME
  ;; ygg-delta-fn / ygg-apply-delta-fn / ygg-clear-delta-fn hooks the G-Set uses.
  (-apply-delta [this delta] (apply-delta this delta))

  p/GarbageCollectable
  (gc-roots [this]
    (async+sync (:sync? opts) (async #{(await (p/snapshot-id this))})))
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids gc-opts]
    (async+sync (:sync? opts)
                (async
                 (await (flush! this))
                 ;; reachable = the graph PSS nodes (from its root + each retained
                 ;; snapshot's graph root) ∪ the live commit blobs (ids drained from
                 ;; the graph) ∪ retained snapshots' ids ∪ the snapshot blobs ∪ the
                 ;; pointer cells. The cutoff rides in `gc-opts`.
                 (let [live-ids   (map first (await (d/set->clj graph opts)))
                       snap-addrs (mapv #(parse-uuid (str %)) snapshot-ids)
                       retained   (loop [as (seq snap-addrs) roots [] ids #{}]
                                    (if as
                                      (let [snap (await (d/read-commit kv-store (first as) opts))
                                            sg   (d/restore-set graph-cmp (:graph snap) storage opts)
                                            sids (map first (await (d/set->clj sg opts)))]
                                        (recur (next as) (conj roots (:graph snap)) (into ids sids)))
                                      {:roots roots :ids ids}))
                       graph-root (await (d/store-set! graph storage opts))]
                   (await (d/gc! kv-store (cons graph-root (:roots retained)) (graph-config config)
                                 (clojure.core/merge gc-opts opts
                                                     {:spare-keys [(gk config) (gfk config) (sk config)]
                                                      :retain-keys (vec (into (set live-ids)
                                                                              (concat (:ids retained) snap-addrs)))}))))))))

(defn flush!
  "Persist the graph PSS (convergent roots write) + the {:heads :version} cache cell.
   Returns a clean record. (async+sync)"
  [cd]
  (async+sync (:sync? (:opts cd))
              (async
               (await (save-graph! (:kv-store cd) (:graph cd) (:storage cd) (:config cd) (:opts cd)))
               (await (save-state! (:kv-store cd) (:state cd) (:config cd) (:opts cd)))
               (assoc cd :dirty false))))

;; ============================================================
;; Factory
;; ============================================================

(defn cdvcs
  "Open (or create) a durable CDVCS. Loads the {:heads :version} cell + restores the
   commit-graph PSS if present; otherwise seeds a fresh single-base-commit CDVCS for
   `:author` and persists it. (async+sync)

   config (DOMAIN): :store-config | :kv-store, :author, :state-key / :graph-key
           (cell keys; defaults :cdvcs/state / :cdvcs/graph).
   opts (RUNTIME): :sync? (default true)."
  ([id] (cdvcs id {} {:sync? true}))
  ([id config] (cdvcs id config {:sync? true}))
  ([id {:keys [author store-config kv-store state-key graph-key]}
    {:keys [sync?] :or {sync? true}}]
   (let [opts {:sync? sync?}
         cell-config (cond-> {} state-key (assoc :state-key state-key)
                             graph-key (assoc :graph-key graph-key))
         ;; co-located instances (a vector :state-key like [:cdvcs/state id] sharing
         ;; ONE store) need DISTINCT graph cells too — derive them from the state-key
         ;; suffix, just as the two-half CRDTs derive :freed-key from :roots-key.
         cell-config (if (and (vector? state-key) (not graph-key))
                       (assoc cell-config
                              :graph-key (assoc state-key 0 :cdvcs/graph)
                              :graph-freed-key (assoc state-key 0 :cdvcs/graph-freed))
                       cell-config)
         open-config (cond-> {} kv-store (assoc :kv-store kv-store))]
     (async+sync sync?
                 (async
                  (let [{:keys [kv-store store-config storage]} (await (d/open store-config open-config opts))
                        graph (await (load-graph kv-store storage cell-config opts))
                        st    (await (kb/k-get kv-store (sk cell-config) opts))]
                    (if st
                      (->CDVCS id kv-store store-config storage graph st false cell-config opts)
                      (let [{bid :id value :value} (core/new-base author)
                            graph' (await (d/set-conj graph [bid []] graph-cmp opts))
                            state  {:heads #{bid} :version 1}]
                        (await (persist-commits! kv-store {bid value} opts))
                        (await (save-graph! kv-store graph' storage cell-config opts))
                        (await (save-state! kv-store state cell-config opts))
                        (->CDVCS id kv-store store-config storage graph' state false cell-config opts)))))))))
