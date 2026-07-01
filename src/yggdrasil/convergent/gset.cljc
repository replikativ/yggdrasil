(ns yggdrasil.convergent.gset
  "Grow-only Set (G-Set) as a DURABLE conflict-free yggdrasil system.

   Same algebra as the in-memory `yggdrasil.convergent.gset` (value = a set,
   join = union) but the value is a persistent-sorted-set over konserve — the
   exact PSS+KonserveStorage substrate the snapshot registry runs on.

   VALUE SEMANTICS — SINGLE CURRENT BRANCH: the record holds ONE branch's `root`
   (the current working PSS set), its `branch` name, `dirty?`, and the current
   branch's TIP `commit` (a commit-DAG id). Other branches live only in the STORE — one
   grow-set REGISTRY (`:crdt/branches`) + one mutable HEAD cell per branch
   (`:crdt.head/<branch>` = `{:root <fused-node> :commit <id>}`). So a flush and an
   open each touch only the current branch (O(1) in branch count); `branch!`/
   `checkout`/`branches`/`merge!` are store-backed (async+sync). The mutable cell for
   the current VALUE lives in the HOLDER — a spindel signal-atom or an overlay's
   `:local-writes` — which swaps this value; the value-wire ships ONE branch, the
   whole fork tree replicates via the store.

   **Fully cross-platform.** The record does NOT carry an execution mode — each
   content-touching op takes an OPTIONAL trailing `opts` ({:sync?}) and defaults to
   `c/default-opts` (SYNC on JVM, ASYNC on cljs)."
  (:refer-clojure :exclude [conj contains?])
  (:require [clojure.set :as set]
            [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.overlay :as ovl]
            [yggdrasil.kbridge :as kb]
            [yggdrasil.fressian :as yf]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->GSet flush! apply-delta)

(defrecord GSet
           [id kv-store store-config storage comparator
            root        ; the CURRENT branch's immutable PSS set (working copy)
            branch      ; current branch keyword
            dirty?      ; has `root` changed since the last flush?
            commit      ; branch TIP commit-id (string) in the commit-DAG; nil before base seed
            config]     ; DOMAIN: cell-keys (:branches-key/:cell-ns); {} ⇒ store defaults

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :gset)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true :overlayable true
                     :graphable true})

  p/Snapshotable
  ;; snapshot-id = the content-addressed PSS ROOT of the current branch (an addressable
  ;; value handle, stable across peers) — so `as-of`/`branch!` can re-open it.
  (snapshot-id [_]
    (async+sync (:sync? c/default-opts)
                (async (str (await (d/materialize-root! root storage c/default-opts))))))
  (parent-ids [_] (if commit #{commit} #{}))
  (as-of [this snap-id] (p/as-of this snap-id c/default-opts))
  (as-of [_ snap-id opts]
    ;; restore the immutable set rooted at `snap-id` (a content root address).
    (async+sync (:sync? opts)
                (async (await (d/set->clj (d/restore-set comparator (parse-uuid (str snap-id)) storage opts) opts)))))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  ;; structural ops — STORE-BACKED (async+sync): branches live in the store, not the record.
  p/Branchable
  (branches [this] (p/branches this c/default-opts))
  (branches [_ opts] (d/load-branches kv-store config opts))
  (current-branch [_] branch)
  (branch! [this name] (p/branch! this name branch c/default-opts))     ; fork from CURRENT
  (branch! [this name from] (p/branch! this name from c/default-opts))
  ;; create branch `name` from `from` (a branch keyword or a snapshot-id string) by writing
  ;; its head cell (root node + parent link) and registering it. The new branch shares the
  ;; immutable content-addressed nodes; writes go to fresh nodes → isolated by construction.
  ;; STAYS on the current branch (checkout to switch).
  (branch! [this name from opts]
    (async+sync (:sync? opts)
                (async
                 ;; the fork INHERITS the source branch's tip commit (datahike: branch! authors
                 ;; no commit — the new branch simply points at the fork-point commit).
                 (let [[node tip]
                       (cond
                         (and (keyword? from) (= from branch))
                         [(d/root-node-blob root) commit]
                         (keyword? from)
                         (let [h (await (d/load-head kv-store from config opts))]
                           [(:root h) (:commit h)])
                         :else
                         [(await (kb/k-get kv-store (parse-uuid (str from)) opts)) commit])]
                   (await (d/save-head! kv-store name {:root node :commit tip} config opts))
                   (await (d/register-branch! kv-store name config opts))
                   this))))
  (delete-branch! [this name] (p/delete-branch! this name c/default-opts))
  (delete-branch! [this name opts]
    (async+sync (:sync? opts)
                (async (await (d/delete-head! kv-store name config opts)) this)))
  (checkout [this name] (p/checkout this name c/default-opts))
  (checkout [this name opts]
    (async+sync (:sync? opts)
                (async
                 ;; flush current first (don't lose work), then load the target head.
                 (let [flushed (await (flush! this opts))
                       h       (await (d/load-head kv-store name config opts))]
                   (assoc flushed :root (d/restore-fused comparator (:root h) storage opts)
                          :branch name :commit (:commit h) :dirty? false)))))

  p/Mergeable
  ;; branch-merge = value union of another branch (loaded from its head) into the current one.
  (merge! [this source] (p/merge! this source c/default-opts))
  (merge! [this source opts]
    (async+sync (:sync? opts)
                (async
                 (let [h         (await (d/load-head kv-store source config opts))
                       src       (d/restore-fused comparator (:root h) storage opts)
                       u         (await (d/set-union root src comparator opts))
                       ;; author a MERGE commit: parents = {this tip, source tip} (datahike merge!)
                       root-addr (str (await (d/materialize-root! u storage opts)))
                       ;; `into` (not a #{} literal) — the two tips may be EQUAL (e.g. neither
                       ;; branch committed since the fork), which a set literal rejects.
                       mc        (d/make-commit root-addr (disj (into #{} [commit (:commit h)]) nil))
                       _         (await (d/append-commit! kv-store mc opts))
                       g'        (assoc this :root u :dirty? true :commit (:id mc))]
                   (if (:flush? opts true) (await (flush! g' opts)) g')))))
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {})

  p/Committable
  ;; DECOUPLED from flush!: flush! just persists the working root to the head cell (per op);
  ;; commit! additionally MATERIALIZES the root as a store node (so the commit has a stable
  ;; :root address), appends a commit `{:root :parents #{prev-tip}}` to the shared DAG, and
  ;; advances the branch tip. (async+sync)
  (commit! [this] (p/commit! this nil c/default-opts))
  (commit! [this message] (p/commit! this message c/default-opts))
  (commit! [this _message opts]
    (async+sync (:sync? opts)
                (async
                 (let [root-addr (str (await (d/materialize-root! root storage opts)))
                       root-node (d/root-node-blob root)
                       mc        (d/make-commit root-addr (if commit #{commit} #{}))]
                   (await (d/append-commit! kv-store mc opts))    ; standalone content-addressed commit key
                   (await (d/save-head! kv-store branch {:root root-node :commit (:id mc)} config opts))
                   (assoc this :dirty? false :commit (:id mc))))))

  c/PConvergent
  (-join [this other] (c/-join this other c/default-opts))
  ;; value join over signal-sync: union the two peers' CURRENT-branch roots.
  (-join [this other opts]
    (async+sync (:sync? opts)
                (async
                 (let [u (await (d/set-union root (:root other) comparator opts))]
                   ;; IDEMPOTENCE: a join that adds nothing returns the receiver UNCHANGED
                   ;; (identical) — else mutually-synced peers re-publish forever.
                   (if (= u root)
                     this
                     (let [g' (->GSet id kv-store store-config storage comparator
                                      u branch true commit config)]
                       (if (:flush? opts true) (await (flush! g' opts)) g')))))))
  (-conflict-free? [_] true)

  c/PDeltaApply
  (-apply-delta [this delta] (apply-delta this delta))
  (-apply-delta [this delta opts] (apply-delta this delta opts))

  p/GarbageCollectable
  (gc-roots [this]
    (async+sync (:sync? c/default-opts) (async #{(await (p/snapshot-id this))})))
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids gc-opts]
    (let [gc-opts (merge c/default-opts (t/async-gc-opts "gset/gc-sweep!" gc-opts))]
      (async+sync (:sync? gc-opts)
                  (async
                   (await (flush! this gc-opts))
                   ;; GC must retain EVERY branch (else it sweeps a sibling branch's live
                   ;; nodes) — enumerate the registry, collect each head's roots, and spare
                   ;; the registry + head cells. `snapshot-ids` are held-snapshot roots
                   ;; (stringified UUIDs → parse for the reachability walk).
                   (let [branches (await (d/load-branches kv-store config gc-opts))
                         ;; ONE pass: the reachable seed (live head roots) + each branch's TIP.
                         {:keys [roots tips]}
                         (loop [bs (seq branches) roots [] tips []]
                           (if bs
                             (let [h (await (d/load-head kv-store (first bs) config gc-opts))]
                               (recur (next bs) (into roots (d/head-roots h)) (clojure.core/conj tips (:commit h))))
                             {:roots roots :tips tips}))
                         ;; datahike-style retention: keep historical commit root trees WITHIN
                         ;; the window (epoch cutoff ⇒ keep them all → reclaim nothing).
                         commit-roots (await (d/commit-reachable-roots kv-store storage tips
                                                                       (t/gc-cutoff gc-opts) config gc-opts))
                         ;; retain the live commit KEYS (full chain from tips); orphaned commits
                         ;; from a deleted branch fall out of the whitelist → reclaimable.
                         commit-keys  (await (d/commit-reachable-keys kv-store tips gc-opts))]
                     (await (d/gc! kv-store roots config
                                   (merge gc-opts
                                          {:retain-roots (into (mapv #(parse-uuid (str %)) snapshot-ids) commit-roots)
                                           :retain-keys  commit-keys
                                           :spare-keys   (d/head-cell-keys config branches)}))))))))

  p/Overlayable
  ;; :frozen → carry the current root (immutable PSS value; isolated by value).
  ;; :following → an EMPTY current root; `ovl/overlay-value` joins it with the LIVE parent
  ;; on read. Mutate the clone via `ovl/overlay-swap!`; `merge-down!` joins it back.
  (overlay [this opts]
    (let [mode (or (:mode opts) :frozen)
          lw   (if (= :following mode)
                 (assoc this :root (d/empty-set storage comparator) :dirty? false)
                 (assoc this :dirty? false))]
      (ovl/convergent-overlay this mode lw)))

  p/Graphable
  ;; the commit-DAG (standalone content-addressed commit keys) — all methods delegate to the
  ;; shared `d/commit-*` helpers (gset + the two-half CRDTs share one implementation).
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
;; Value ops — each returns a NEW G-Set value (value-semantic)
;; ============================================================

(defn conj
  "Add `x` to the current branch's set; RECORD the op as a local δ (`{x}`) so a
   synced signal can ship just the op. (async+sync) Returns a NEW (δ-carrying) g."
  ([g x] (conj g x c/default-opts))
  ([g x opts]
   (async+sync (:sync? opts)
               (async
                (let [s' (await (d/set-conj (:root g) x (:comparator g) opts))
                      g' (c/with-delta (assoc g :root s' :dirty? true) set/union #{x})]
                  ;; AUTO-FLUSH (`:flush?` default true): commit so the value is a durable,
                  ;; sendable root. `{:flush? false}` batches a run of ops + one `flush!`.
                  (if (:flush? opts true) (await (flush! g' opts)) g'))))))

(defn apply-delta
  "Consume a peer's G-Set δ — a set of added elements — by unioning it into the
   current branch. The OP-path apply (cheap: O(δ)); counterpart to -join. (async+sync)"
  ([g delta] (apply-delta g delta c/default-opts))
  ([g delta opts]
   (async+sync (:sync? opts)
               (async
                (let [s' (loop [s (:root g) es (seq delta)]
                           (if es
                             (recur (await (d/set-conj s (first es) (:comparator g) opts)) (next es))
                             s))
                      ;; clear δ: a remote-integrated value re-propagates nothing.
                      g' (c/clear-delta (assoc g :root s' :dirty? true))]
                  (if (:flush? opts true) (await (flush! g' opts)) g'))))))

(defn elements
  "Read the current branch's set as a plain Clojure set. (async+sync)"
  ([g] (elements g c/default-opts))
  ([g opts] (d/set->clj (:root g) opts)))

(defn contains?
  "Whether `x` is in the current branch's set. (async+sync)"
  ([g x] (contains? g x c/default-opts))
  ([g x opts] (d/set-contains? (:root g) x opts)))

(defn added
  "Element-level join-delta: elements in `g` not in peer `other`. (async+sync)"
  ([g other] (added g other c/default-opts))
  ([g other opts]
   (async+sync (:sync? opts)
               (async (set/difference (await (elements g opts)) (await (elements other opts)))))))

(defn merge-base
  "The most recent common ancestor commit of branches `a` and `b` (git merge-base) — LCA over
   the standalone commit keys, seeded from each branch's tip. nil if no shared ancestor.
   (async+sync)"
  ([g a b] (merge-base g a b c/default-opts))
  ([g a b opts] (d/commit-merge-base (:kv-store g) (:storage g) (:config g) a b opts)))

;; ============================================================
;; Persistence — cross-platform
;; ============================================================

(defn flush!
  "Persist the current branch's set into its head cell (root fused, children written).
   Returns a NEW g with `dirty?` cleared (callers must ADOPT it). (async+sync)"
  ([g] (flush! g c/default-opts))
  ([g opts]
   (async+sync (:sync? opts)
               (async
                (if (:dirty? g)
                  (let [{:keys [root-node]} (await (d/flush-set-fused! (:root g) (:storage g) opts))]
                    (await (d/save-head! (:kv-store g) (:branch g)
                                         {:root root-node :commit (:commit g)} (:config g) opts))
                    (assoc g :dirty? false))
                  g)))))

;; ============================================================
;; Cross-store sync + GC — cross-platform
;; ============================================================

(defn merge-peer!
  "Reconcile with a peer G-Set in a DIFFERENT store — the durable, cross-STORE form of
   -join over the WHOLE tree (merge-peer! holds both stores; the value-wire is one branch,
   but a store reconcile carries every branch). For each branch the peer has, ship its
   nodes here, restore, and union into the same-named local branch's head cell (creating
   it). Returns a NEW g on its (possibly-merged) current branch. (async+sync)"
  ([g other] (merge-peer! g other c/default-opts))
  ([g other opts]
   (async+sync (:sync? opts)
               (async
                (let [g         (await (flush! g opts))   ; flush current before touching head cells
                      cmp       (:comparator g)
                      o-store   (:kv-store other) o-config (:config other)
                      g-store   (:kv-store g)     g-storage (:storage g)     g-config (:config g)
                      obranches (await (d/load-branches o-store o-config opts))]
                  ;; CURRENT branch accumulates into g's in-memory (resident) root; OTHER
                  ;; branches merge into their head cells — so the returned value is never a
                  ;; lazy restore. Ship each peer head's fused NODE tree here directly.
                  (loop [bs (seq obranches) cur (:root g)]
                    (if bs
                      (let [b     (first bs)
                            oh    (await (d/load-head o-store b o-config opts))
                            _     (await (d/ship-node! o-store g-store (:root oh) opts))
                            ;; ship the peer branch's LINEAGE (standalone commit keys + root trees)
                            _     (await (d/ship-commits! o-store g-store (:commit oh) opts))
                            orest (d/restore-fused cmp (:root oh) g-storage opts)]
                        (if (= b (:branch g))
                          (recur (next bs) (await (d/set-union cur orest cmp opts)))
                          (let [gh (await (d/load-head g-store b g-config opts))
                                u  (await (d/set-union (d/restore-fused cmp (:root gh) g-storage opts) orest cmp opts))
                                {rn :root-node} (await (d/flush-set-fused! u g-storage opts))]
                            (await (d/save-head! g-store b {:root rn :commit (or (:commit gh) (:commit oh))} g-config opts))
                            (await (d/register-branch! g-store b g-config opts))
                            (recur (next bs) cur))))
                      (let [g' (assoc g :root cur :dirty? true)]
                        (if (:flush? opts true) (await (flush! g' opts)) g')))))))))

(defn gc!
  "Reclaim PSS nodes superseded by prior flushes (mark-and-sweep). SAFE BY DEFAULT
   (reclaims nothing); pass a window in `opts` (`:remove-before`/`:grace-period-ms`).
   (async+sync)"
  ([g] (p/gc-sweep! g nil nil))
  ([g opts] (p/gc-sweep! g nil opts)))

;; ============================================================
;; Factory — cross-platform
;; ============================================================

(defn gset
  "Open (or create) a durable G-Set on a per-system konserve store. Loads ONLY the
   branch registry + the current branch's head (O(1) in branch count). (async+sync —
   pass `:sync? false` on cljs and `await`.)

     (gset \"kb\" {:store-config {:backend :memory :id (random-uuid)}})"
  ([id] (gset id {} {:sync? true}))
  ([id config] (gset id config {:sync? true}))
  ([id {:keys [store-config comparator branch kv-store branches-key cell-ns]
        :or {comparator compare branch :main}}
    {:keys [sync?] :or {sync? true}}]
   (let [store-config (or store-config (when-not kv-store (d/mem-store-config)))
         open-opts {:sync? sync?}
         ;; the PERSISTENT domain the record carries: the cell-keys (constant).
         cell-config (cond-> {}
                       branches-key (assoc :branches-key branches-key)
                       cell-ns (assoc :cell-ns cell-ns))
         open-config (cond-> cell-config kv-store (assoc :kv-store kv-store))]
     (async+sync sync?
                 (async
                  (let [{:keys [kv-store storage]} (await (d/open store-config open-config open-opts))
                        registry (await (d/load-branches kv-store cell-config open-opts))
                        ;; pick the current branch: the requested one if it exists, else
                        ;; :main, else any existing, else the requested (fresh store).
                        cur (cond (registry branch) branch
                                  (registry :main)  :main
                                  (seq registry)    (first registry)
                                  :else             branch)
                        h    (await (d/load-head kv-store cur cell-config open-opts))
                        root (d/restore-fused comparator (:root h) storage open-opts)]
                    ;; a fresh store has no registry yet — register the current branch.
                    (when-not (seq registry)
                      (await (d/register-branch! kv-store cur cell-config open-opts)))
                    ;; seed the canonical base commit ONCE per store, so every branch traces to
                    ;; a shared common ancestor (merge-base is never nil for same-origin CRDTs).
                    (let [base (d/base-commit)]
                      (when-not (await (d/read-commit kv-store (:id base) open-opts))
                        (await (d/append-commit! kv-store base open-opts)))
                      (->GSet id kv-store store-config storage comparator
                              root cur false (or (:commit h) (:id base)) cell-config))))))))

;; Register the G-Set with the system value codec. The VALUE is the CURRENT branch only —
;; its root node rides inline (fused; the receiver seeds it), the whole tree replicates via
;; the store. storage/kv-store/comparator are runtime/derived and re-injected on read.
(yf/register-system!
 :gset GSet
 (fn [{:keys [id store-config root branch commit config]}]
   {:id id :store-config store-config
    :root (d/root-node-blob root)      ; current branch's root node inline, nil if empty
    :branch branch :commit commit :config config})
 (fn [blob storage opts]
   (->GSet (:id blob) (:kv-store storage) (:store-config blob) storage compare
           (d/restore-fused compare (:root blob) storage opts)
           (:branch blob) false (:commit blob) (:config blob))))
