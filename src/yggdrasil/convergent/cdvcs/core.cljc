(ns yggdrasil.convergent.cdvcs.core
  "Pure, value-based CDVCS verbs — ported from replikativ.crdt.cdvcs.core, with
   replikativ's kabel staging (`:prepared`/`:downstream`/`:new-values` split)
   collapsed and its deps swapped for yggdrasil's: `*id-fn*` → hasch content
   hash, `*date-fn*` → `yggdrasil.types/now-ms` (HLC-injectable, replay-safe).

   Every verb returns `{:state {:commit-graph :heads :version} :commits {id val}}`
   — `:state` is the convergent value (joinable via `graph/downstream`), `:commits`
   accrues the content-addressed commit blobs the caller persists. A commit id IS
   the hash of `{:transactions :parents}`, so identical content ⇒ identical id ⇒
   idempotent additions (the grow-only-graph CRDT property).

   `commit` is single-head only (throws on multiple heads — commit your merge
   first). `merge` reconciles multiple heads into one via a merge commit. `pull`
   fast-forwards and refuses to induce a conflict. The whole point: `graph/downstream`
   always converges (may leave >1 head); `merge` is the explicit, authored
   resolution."
  (:refer-clojure :exclude [merge])
  (:require [clojure.set :as set]
            [hasch.core :as hasch]
            [yggdrasil.types :as t]
            [yggdrasil.convergent.cdvcs.graph :as graph]))

(defn- commit-id [commit-value]
  (hasch/uuid (select-keys commit-value #{:transactions :parents})))

(defn make-commit
  "Build a single commit value + its content-addressed id (`hash {:transactions
   :parents}`). Pure — the durable layer persists the blob and appends `[id parents]`
   to the commit-graph. Used by the PSS-backed (Option B) durable wrapper."
  [author parents transactions]
  (let [cval {:transactions transactions :parents (vec parents) :crdt :cdvcs
              :version 1 :ts (t/now-ms) :author author}]
    {:id (commit-id cval) :value cval}))

(defn new-base
  "The base commit (no transactions, no parents) — a CANONICAL shared sentinel:
   author/ts-independent in BOTH id AND value (fixed `:ts 0`, `:author nil`), so ALL
   fresh CDVCS share ONE byte-identical base ⇒ a common ancestor ⇒ always joinable.
   The value-determinism matters once commit values are inlined in the graph PSS:
   identical base value ⇒ identical leaf ⇒ identical content-addressed root across
   peers (a per-author/ts base would diverge the root). `author` is ignored — the
   base is unattributed by design (its id never depended on it)."
  [_author]
  (let [cval {:transactions [] :parents [] :crdt :cdvcs :version 1 :ts 0 :author nil}]
    {:id (commit-id cval) :value cval}))

(defn new-cdvcs
  "A fresh CDVCS with a single empty base commit. Returns {:state :commits}."
  [author]
  (let [base {:transactions [] :parents [] :crdt :cdvcs :version 1
              :ts (t/now-ms) :author author}
        id   (commit-id base)]
    {:state   {:commit-graph {id []} :heads #{id} :version 1}
     :commits {id base}}))

(defn fork
  "Fork (clone) a remote CDVCS value as a working copy. Returns {:state :commits}."
  [remote-state]
  {:state (select-keys remote-state [:commit-graph :heads :version])
   :commits {}})

(defn multiple-heads? [state]
  (> (count (:heads state)) 1))

(defn- raw-commit
  "Append a commit with `transactions` and an explicit ordered `parents` vector.
   Returns {:state :commits} (`:commits` = the single new blob)."
  [state parents author transactions]
  (let [parents (vec parents)
        cval    {:transactions transactions :parents parents :crdt :cdvcs
                 :version 1 :ts (t/now-ms) :author author}
        id      (commit-id cval)
        state'  (-> state
                    (assoc-in [:commit-graph id] parents)
                    (update :heads set/difference (set parents))
                    (update :heads conj id))]
    {:state state' :commits {id cval}}))

(defn commit
  "Commit `transactions` onto a single-head CDVCS. Throws on multiple heads."
  [state author transactions]
  (let [heads (:heads state)]
    (if (= 1 (count heads))
      (raw-commit state (vec heads) author transactions)
      (throw (ex-info "CDVCS has multiple heads — merge before committing."
                      {:type :multiple-heads :heads heads})))))

(defn merge-heads
  "An ordered vector of the union of both head sets (reorder to pick parent order)."
  [state-a state-b]
  (vec (distinct (concat (:heads state-a) (:heads state-b)))))

(defn pull
  "Fast-forward `state` to include `remote-tip` and its ancestors from
   `remote-state`. Throws if the remote is not a superset (would need a merge) or
   if pulling would induce multiple heads (unless `allow-induced-conflict?`)."
  ([state remote-state remote-tip]
   (pull state remote-state remote-tip false))
  ([state remote-state remote-tip allow-induced-conflict?]
   (when (get-in state [:commit-graph remote-tip])
     (throw (ex-info "No pull necessary — remote-tip already present."
                     {:type :pull-unnecessary :remote-tip remote-tip})))
   (let [{heads :heads graph :commit-graph} state
         {:keys [lcas]} (graph/lowest-common-ancestors
                         graph heads (:commit-graph remote-state) #{remote-tip})
         new-graph (clojure.core/merge (:commit-graph remote-state) graph)
         state'    (-> state
                       (assoc :commit-graph new-graph)
                       (assoc :heads (graph/remove-ancestors
                                      new-graph graph heads #{remote-tip})))]
     (when (and (not allow-induced-conflict?) (not (set/superset? lcas heads)))
       (throw (ex-info "Remote is not pullable (not a superset) — use merge."
                       {:type :not-superset :lcas lcas :heads heads})))
     (when (and (not allow-induced-conflict?) (multiple-heads? state'))
       (throw (ex-info "Cannot pull without inducing a conflict — use merge."
                       {:type :multiple-heads :heads (:heads state')})))
     ;; the missing commit blobs are content-addressed; the caller ships them
     ;; (konserve-sync) — pure layer just advances the convergent value.
     {:state state' :commits {}})))

(defn merge
  "Reconcile a CDVCS with `remote-state` (or its own multiple heads): join the
   graphs, then record a merge commit whose parents are all the heads. Returns
   {:state :commits}. `correcting-transactions` (default []) are committed in the
   merge commit to resolve the divergence at the value level."
  ([state author remote-state]
   (merge state author remote-state (merge-heads state remote-state) []))
  ([state author remote-state heads correcting-transactions]
   (let [heads-needed (set/union (:heads state) (:heads remote-state))]
     (when-not (= heads-needed (set heads))
       (throw (ex-info "Provided heads don't match the heads needing a merge."
                       {:type :heads-dont-match :heads heads :needed heads-needed})))
     (let [{:keys [visited-b]} (graph/lowest-common-ancestors
                                (:commit-graph state) (:heads state)
                                (:commit-graph remote-state) (:heads remote-state))
           new-graph (clojure.core/merge (:commit-graph state)
                                         (select-keys (:commit-graph remote-state)
                                                      visited-b))]
       (raw-commit (assoc state :commit-graph new-graph)
                   (vec heads) author correcting-transactions)))))
