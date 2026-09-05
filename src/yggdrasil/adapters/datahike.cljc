(ns yggdrasil.adapters.datahike
  "Datahike adapter for Yggdrasil protocols.

  Wraps a Datahike connection (atom of db state) and exposes
  Snapshotable, Branchable, Graphable, Mergeable, and GarbageCollectable.

  Also extends the yggdrasil.hooks multimethod to use datahike's native
  d/listen for immediate commit notification (no polling needed).

  Requires datahike on the classpath. Only load this namespace when
  datahike is available as a dependency."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.hooks :as hooks]
            ;; the shared core.async↔partial-cps adapter: bridges datahike's (core.async)
            ;; async results into yggdrasil's NATIVE partial-cps substrate, so the adapter's
            ;; storage ops are `await`-able by the convergent/composite layer (and equally
            ;; bridged by spindel's distributed.cps) — not ad-hoc core.async.
            [is.simm.partial-cps.core-async :as ca]
            [konserve.core :as k]
            ;; partial-cps async substrate (async/await + the async+sync duality macro):
            ;; one body → JVM-sync (values) + cljs-async (CPS). Same idiom the convergent
            ;; CRDTs / composite / storage layer use.
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]])
            [datahike.api :as d]
            [datahike.versioning :as dv]
            ;; fressian is the JVM-only value codec (org.fressian); the
            ;; register-system! call at the bottom is the only user, gated :clj.
            #?@(:clj [[yggdrasil.fressian :as yf]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

;; ============================================================
;; Internal helpers
;; ============================================================

(defn- store-of [conn]
  (:store @conn))

(defn- sync?*
  "Resolve the :sync? mode for a storage-touching op: an explicit `:sync?` opt wins,
   else the platform default — JVM blocks and returns values, cljs returns a
   partial-cps CPS (`await`-able). Thread the result into both `async+sync` and the
   konserve/datahike calls so one body serves both platforms + any backend."
  [opts]
  (get opts :sync? #?(:clj true :cljs false)))

(defn- reclaimed-count
  "Normalize datahike gc-storage's return (a count, or the collection of removed
   addresses) to an integer key-count."
  [removed]
  (cond (number? removed)   removed
        (counted? removed)  (count removed)
        (seqable? removed)  (count (seq removed))
        :else 0))

(defn- db-of [conn]
  @conn)

(defn- branch-of [conn]
  (get-in @conn [:config :branch]))

(defn- commit-id-of [db]
  (get-in db [:meta :datahike/commit-id]))

(defn- parent-ids-of [db]
  (get-in db [:meta :datahike/parents]))

(defn- snapshot-meta-of-stored [raw-db]
  {:snapshot-id (str (commit-id-of raw-db))
   :parent-ids (set (map str (parent-ids-of raw-db)))
   :timestamp (get-in raw-db [:meta :datahike/updated-at])
   :branch (get-in raw-db [:config :branch])})

;; ============================================================
;; Branch diff computation
;; ============================================================

(defn- compute-branch-diff
  "Compute datoms in source-db that are not in target-db.
   Returns vector of [:db/add e a v] transaction data."
  [source-db target-db]
  (let [diff (d/q '[:find ?e ?a ?v
                    :in $ $2
                    :where
                    [$ ?e ?a ?v]
                    [(not= :db/txInstant ?a)]
                    (not [$2 ?e ?a ?v])]
                  source-db target-db)]
    (mapv (fn [[e a v]] [:db/add e a v]) diff)))

;; --- identity-keyed merge (sibling-safe) ----------------------------------
;; Raw [:db/add e a v] tx-data collides across SIBLING branches: each branch
;; allocates entity-ids sequentially from the shared fork point, so two forks
;; mint the SAME ?e for different entities → the second merge clobbers the
;; first. The fix: address merged entities by their `:db.unique/identity`
;; lookup-ref, so datahike unions them by SEMANTIC id. (Additions only here —
;; append-only data like a conversation. Entities CHANGED on both sides are a
;; 3-way conflict, surfaced via `conflicts` + reconciled by an agent.)

(defn- schema-attrs
  "{:unique #{idents} :ref #{idents} :component #{idents}} for db's schema."
  [db]
  (let [rows (d/q '[:find ?id ?uniq ?vt ?comp
                    :where
                    [?a :db/ident ?id]
                    [(get-else $ ?a :db/unique :none) ?uniq]
                    [(get-else $ ?a :db/valueType :none) ?vt]
                    [(get-else $ ?a :db/isComponent false) ?comp]]
                  db)]
    {:unique    (set (keep (fn [[id u]]    (when (= u :db.unique/identity) id)) rows))
     :ref       (set (keep (fn [[id _ vt]] (when (= vt :db.type/ref) id)) rows))
     :component (set (keep (fn [[id _ _ c]] (when c id)) rows))}))

(defn- entity-ident
  "Lookup-ref [uattr uval] identifying entity `e`, or nil if it has none."
  [db unique e]
  (let [ent (d/entity db e)]
    (some (fn [ua] (when-some [uv (get ent ua)] [ua uv])) unique)))

(defn- resolve-db
  "Resolve a branch keyword / commit-uuid / snapshot-id string to a db value."
  [store x]
  (cond
    (keyword? x) (dv/branch-as-db store x)
    (uuid? x)    (dv/commit-as-db store x)
    :else        (when-let [u (parse-uuid (str x))] (dv/commit-as-db store u))))

(defn- card-one-attrs
  "Cardinality-one, non-unique-identity attrs — the ones a 3-way merge can
   genuinely conflict on (a unique id never conflicts; cardinality-many unions)."
  [db]
  (set (d/q '[:find [?id ...]
              :where
              [?a :db/ident ?id]
              [(get-else $ ?a :db/cardinality :db.cardinality/one) ?c]
              [(= ?c :db.cardinality/one)]
              (not [?a :db/unique :db.unique/identity])]
            db)))

(defn- card-many-attrs
  "Cardinality-many attrs. They normally merge by union, but are relevant to
   conflict detection when the entity itself was deleted on the other side."
  [db]
  (set (d/q '[:find [?id ...]
              :where
              [?a :db/ident ?id]
              [?a :db/cardinality :db.cardinality/many]]
            db)))

(defn- compute-conflicts
  "Precise 3-way conflicts for semantic entities and anonymous entities inherited
   from BASE. A cardinality-one attr conflicts when both descendants changed it
   from BASE to different values. Deleting a BASE entity is an existence change:
   if the surviving side changes any attribute (including adding one that was
   absent in BASE), that attribute conflicts with the tombstone. This prevents a
   blind merge from leaving only the new attribute behind as an orphan. A pure
   deletion against an unchanged entity and an ordinary one-sided attribute add
   remain conflict-free.

   Identity-bearing entities are addressed by a lookup ref. An anonymous base
   entity is addressed by the opaque descriptor [:yggdrasil/base-eid e]. Newly
   created anonymous entities on sibling branches are deliberately unrelated,
   even when Datahike happened to allocate the same numeric eid."
  [base-db ours-db theirs-db]
  (let [{base-unique :unique base-ref :ref} (schema-attrs base-db)
        {ours-unique :unique ours-ref :ref} (schema-attrs ours-db)
        {theirs-unique :unique theirs-ref :ref} (schema-attrs theirs-db)
        unique  (into base-unique (concat ours-unique theirs-unique))
        ref      (into base-ref (concat ours-ref theirs-ref))
        cattrs   (into (card-one-attrs base-db)
                       (concat (card-one-attrs ours-db)
                               (card-one-attrs theirs-db)))
        mattrs   (into (card-many-attrs base-db)
                       (concat (card-many-attrs ours-db)
                               (card-many-attrs theirs-db)))
        exists?  (fn [db e] (and e (seq (d/datoms db :eavt e))))
        find-e   (fn [db [ua uv]]
                   (d/q '[:find ?e . :in $ ?ua ?uv :where [?e ?ua ?uv]] db ua uv))
        identities
        (set (mapcat (fn [db]
                       (for [ua unique
                             [_e uv] (d/q '[:find ?e ?uv :in $ ?ua
                                            :where [?e ?ua ?uv]] db ua)]
                         [ua uv]))
                     [base-db ours-db theirs-db]))
        base-anonymous
        (->> (d/datoms base-db :eavt)
             (keep (fn [dt]
                     (let [e (:e dt) a (:a dt)]
                       (when (and (not= :db/txInstant a)
                                  (not= "db" (namespace a))
                                  (nil? (entity-ident base-db unique e)))
                         e))))
             set)
        specs    (concat
                  (map (fn [ident]
                         {:descriptor ident
                          :base-e (find-e base-db ident)})
                       identities)
                  (map (fn [e]
                         {:descriptor [:yggdrasil/base-eid e]
                          :base-e e
                          :anonymous? true})
                       base-anonymous))
        branch-e (fn [db {:keys [descriptor base-e anonymous?]}]
                   (if anonymous?
                     (when (exists? db base-e) base-e)
                     (find-e db descriptor)))
        ;; Entity objects belong to one immutable DB value and are never equal
        ;; across descendants. Canonicalize refs to semantic identities, shared
        ;; base eids, or side-qualified branch-local eids.
        canonical-ref
        (fn [db side e]
          ;; The identity an inherited anonymous entity gains in only one
          ;; descendant does not change the semantic identity of references
          ;; that already pointed at it in BASE. Prefer BASE's classification.
          (or (entity-ident base-db unique e)
              (when (exists? base-db e) [:yggdrasil/base-eid e])
              (entity-ident db unique e)
              [:yggdrasil/branch-eid side e]))
        valof    (fn [db side e a]
                   (when e
                     (let [values (map (fn [dt]
                                         (let [v (:v dt)]
                                           (if (and (ref a) (integer? v))
                                             (canonical-ref db side v)
                                             v)))
                                       (d/datoms db :eavt e a))]
                       (if (mattrs a)
                         (set values)
                         (first values)))))
        spec-of   (fn [db side e]
                    ;; Prefer the BASE classification. An inherited anonymous
                    ;; entity may acquire a unique identity on one branch, but
                    ;; it is still the same pre-fork object for this merge.
                    (if-let [ident (entity-ident base-db unique e)]
                      {:descriptor ident :base-e e}
                      (if (exists? base-db e)
                        {:descriptor [:yggdrasil/base-eid e]
                         :base-e e :anonymous? true}
                        (if-let [ident (entity-ident db unique e)]
                          {:descriptor ident :base-e (find-e base-db ident)}
                          {:descriptor [:yggdrasil/branch-eid side e]}))))
        descriptor-e
        (fn [db descriptor]
          (case (first descriptor)
            :yggdrasil/base-eid
            (let [e (second descriptor)] (when (exists? db e) e))
            :yggdrasil/branch-eid nil
            (find-e db descriptor)))
        ordinary
        (for [spec specs
              :let  [eb (:base-e spec)
                     eo (branch-e ours-db spec)
                     et (branch-e theirs-db spec)
                     ;; Cardinality-many values union unless deletion of the
                     ;; containing entity competes with a modification. In that
                     ;; case silently preserving only the added datom would create
                     ;; an orphan, so include many attrs in the conflict test.
                     attrs (if (and eb (or (nil? eo) (nil? et)))
                             (into cattrs mattrs)
                             cattrs)]
              a     attrs
              :let  [bv (valof base-db :base eb a)
                     ov (valof ours-db :ours eo a)
                     tv (valof theirs-db :theirs et a)]
              :let  [attribute-conflict?
                     (and (not= ov bv) (not= tv bv) (not= ov tv))
                     tombstone-conflict?
                     (and eb
                          ;; Exactly one descendant deleted the BASE entity. The
                          ;; survivor's value differing from BASE is a concurrent
                          ;; modification even when BASE had no value for `a`.
                          (not= (some? eo) (some? et))
                          (not= (if eo ov tv) bv))]
              ;; both sides changed the attribute differently, or one deleted the
              ;; entity while the survivor changed this attribute …
              :when (and (or attribute-conflict? tombstone-conflict?)
                         ;; … but a temporal attr both sides merely advanced
                         ;; (updated-at, last-seen) is churn, not a semantic clash —
                         ;; the union takes the later value, no reconciliation needed.
                         (not (and (inst? ov) (inst? tv))))]
          {:entity (:descriptor spec) :attr a :base bv :ours ov :theirs tv})
        incoming-ref-conflicts
        (fn [source-db source-side deleted-db]
          (for [dt (d/datoms source-db :eavt)
                :let [e (:e dt) a (:a dt) v (:v dt)]
                :when (and (ref a) (integer? v)
                           (not= "db" (namespace a)))
                :let [subject (spec-of source-db source-side e)
                      referent (canonical-ref source-db source-side v)
                      base-referent (descriptor-e base-db referent)
                      base-value (valof base-db :base (:base-e subject) a)
                      source-value (valof source-db source-side e a)
                      added? (if (mattrs a)
                               (and (contains? source-value referent)
                                    (not (contains? (or base-value #{}) referent)))
                               (and (= referent source-value)
                                    (not= referent base-value)))]
                ;; Adding an incoming edge and deleting its inherited referent
                ;; are changes to different subjects, so the ordinary per-entity
                ;; tombstone test cannot see their interaction. Never translate
                ;; that missing referent to a fresh, empty tempid.
                :when (and base-referent added?
                           (nil? (descriptor-e deleted-db referent)))]
            {:entity (:descriptor subject)
             :attr a
             :base base-value
             :source source-value
             :deleted-referent referent}))
        ref-conflicts
        (concat
         (map (fn [{:keys [entity attr base source deleted-referent]}]
                {:entity entity :attr attr :base base
                 :ours source :theirs nil
                 :reason :deleted-referent :referent deleted-referent})
              (incoming-ref-conflicts ours-db :ours theirs-db))
         (map (fn [{:keys [entity attr base source deleted-referent]}]
                {:entity entity :attr attr :base base
                 :ours nil :theirs source
                 :reason :deleted-referent :referent deleted-referent})
              (incoming-ref-conflicts theirs-db :theirs ours-db)))
        identity-groups
        (fn [db]
          (->> unique
               (mapcat (fn [ua]
                         (d/q '[:find ?e ?ua ?uv
                                :in $ ?ua
                                :where [?e ?ua ?uv]]
                              db ua)))
               (group-by first)
               vals
               (mapv (fn [rows]
                       (set (map (fn [[_ ua uv]] [ua uv]) rows))))))
        base-groups (identity-groups base-db)
        ours-groups (identity-groups ours-db)
        theirs-groups (identity-groups theirs-db)
        partition-view
        (fn [groups identities]
          (->> groups
               (map #(set (filter identities %)))
               (remove empty?)
               distinct
               (sort-by pr-str)
               vec))
        split-conflicts
        (fn [source-groups target-groups source-key target-key]
          (for [identities source-groups
                :when (> (count identities) 1)
                :let [target-view (partition-view target-groups identities)]
                ;; A missing identity is an ordinary one-sided addition. The
                ;; structural conflict is specifically that TARGET already
                ;; resolves every identity, but to several distinct entities.
                :when (and (= identities (reduce into #{} target-view))
                           (> (count target-view) 1))
                :let [source-view [(set identities)]
                      base-view (partition-view base-groups identities)]]
            (merge {:entity (first (sort-by pr-str identities))
                    :attr :db.unique/identity
                    :base base-view
                    :reason :split-identity
                    :identities (vec (sort-by pr-str identities))}
                   {source-key source-view target-key target-view})))
        structural-conflicts
        (concat (split-conflicts ours-groups theirs-groups :ours :theirs)
                (split-conflicts theirs-groups ours-groups :theirs :ours))]
    (->> (concat ordinary ref-conflicts structural-conflicts)
         distinct
         vec)))

(defn- compute-conflicts-baseless
  "Conservative 2-way conflict set, used when the merge-base is UNAVAILABLE — e.g.
   its snapshot was reclaimed by a GC retention window, or the branches share no
   common ancestor. Without a base we cannot tell WHICH side changed a value, so we
   flag every cardinality-one attr whose value DIFFERS between the two heads for the
   same identity-bearing entity. This OVER-flags (a one-sided change looks like a
   clash) on purpose: better to escalate a stale-fork merge to review than to let
   the conflict gate silently see `[]` and blind-merge. Entries are tagged
   `:base :unavailable` so callers can tell this from a true 3-way result."
  [ours-db theirs-db]
  (let [{:keys [unique ref]} (schema-attrs theirs-db)
        cattrs (card-one-attrs theirs-db)
        find-e (fn [db ua uv] (ffirst (d/q '[:find ?e :in $ ?ua ?uv :where [?e ?ua ?uv]] db ua uv)))
        valof  (fn [db e a]
                 (let [v (get (d/entity db e) a)]
                   (if (and v (ref a)) (entity-ident db unique (:db/id v)) v)))]
    (vec
     (for [ua    unique
           [_te uv] (d/q '[:find ?e ?uv :in $ ?ua :where [?e ?ua ?uv]] theirs-db ua)
           :let  [eo (find-e ours-db ua uv)
                  et (find-e theirs-db ua uv)]
           :when (and eo et)
           a     cattrs
           :let  [ov (valof ours-db eo a)
                  tv (valof theirs-db et a)]
           ;; both heads carry a value and they DIFFER (temporal churn excepted —
           ;; updated-at/last-seen just advance, the union takes the later one).
           :when (and (some? ov) (some? tv) (not= ov tv)
                      (not (and (inst? ov) (inst? tv))))]
       {:entity [ua uv] :attr a :base :unavailable :ours ov :theirs tv}))))

(defn- compute-merge-tx
  "Merge tx-data for datoms in source not in target, addressed by SEMANTIC
   identity so concurrent branches union instead of colliding on entity-id.

   Every entity — as a datom's SUBJECT and as a ref VALUE — is addressed by its
   `:db.unique/identity` lookup-ref when it exists in the target, by its shared
   raw eid when an anonymous entity predates the fork, or by a fresh tempid when
   it is new to the source branch. Co-created new entities therefore link via
   the SAME tempid and resolve in one transaction. Inherited anonymous
   components are not replayed as fresh entities on every merge.

   An entity may carry SEVERAL unique-identity attrs (dvergr fuses `:chat/id` and
   `:room/slug` on one entity). We address it by an ALREADY-EXISTING one when any
   exists (so it resolves to that target entity). A missing additional identity
   is asserted onto that entity; an identity already owned by another target
   entity is omitted because `compute-conflicts` reports that split identity
   explicitly. This avoids both silent identity loss and conflicting upserts."
  [source-db target-db base-db]
  (let [{:keys [unique ref]} (schema-attrs source-db)
        tgt-eid    (memoize
                    (fn [[ua uv]]
                      (d/q '[:find ?t . :in $ ?ua ?uv :where [?t ?ua ?uv]] target-db ua uv)))
        in-target? (fn [ua uv] (some? (tgt-eid [ua uv])))
        idents     (fn [e] (let [ent (d/entity source-db e)]
                             (keep (fn [ua] (when-some [uv (get ent ua)] [ua uv])) unique)))
        ;; the TARGET entity a source entity denotes, via shared identity
        tgt-of     (memoize (fn [e] (some tgt-eid (idents e))))
        ;; Entity ids allocated before the fork denote the same anonymous
        ;; component on both descendants. New anonymous entities cannot be
        ;; addressed by raw eid because sibling branches may reuse that number.
        base-e?    (memoize
                    (fn [e]
                      (and base-db
                           (boolean (seq (d/datoms base-db :eavt e))))))
        target-e?  (memoize
                    (fn [e]
                      (boolean (seq (d/datoms target-db :eavt e)))))
        addr       (fn [e]
                     ;; prefer an identity that ALREADY exists in target (→ that
                     ;; entity); else a fresh tempid (new/anonymous)
                     (let [ids (idents e)]
                       (if-let [ex (first (filter (fn [[ua uv]] (in-target? ua uv)) ids))]
                         (vec ex)
                         (if (and (base-e? e) (target-e? e))
                           e
                           (str "ygg-tmp-" e)))))
        ;; SELECTION IS BY IDENTITY, NOT ENTITY ID.
        ;;
        ;; This used to ask datalog for datoms in source `(not [$target ?e ?a ?v])`
        ;; — comparing the raw ?e across two DIVERGED dbs. Entities that predate
        ;; the fork do share an id, but both sides then allocate new ids from the
        ;; same counter, so `new entity 8` on the fork and `new entity 8` on trunk
        ;; are different things. Whenever those numbers coincided AND the value was
        ;; low-cardinality (`:block/order "a0"`, a ref to eid 7, an amount of 42),
        ;; the fork's datom was judged "already in target" and silently dropped —
        ;; `merge!` still returning :ok, the row left orphaned and invisible to
        ;; every join-based read. The ADDRESSING below was always identity-based;
        ;; only this selection was not, so it addressed a wrongly-chosen set.
        ;;
        ;; A source datom is already in target iff target holds the same attribute
        ;; and value on the entity carrying the SAME IDENTITY — with ref values
        ;; mapped through identity too, since a raw referent id is meaningless
        ;; for branch-created entities. An anonymous entity inherited from the
        ;; merge base is safely matched by its shared pre-fork eid; only newly
        ;; created anonymous entities remain deliberately unmatchable.
        present?   (fn [e a v]
                     (when-let [te (or (tgt-of e)
                                       (when (and (base-e? e) (target-e? e)) e))]
                       (let [tv (if (and (ref a) (integer? v))
                                  (or (tgt-of v)
                                      (when (and (base-e? v) (target-e? v)) v))
                                  v)]
                         (and (some? tv)
                              (boolean (seq (d/datoms target-db :eavt te a tv)))))))
        changed?   (fn [e a v]
                     ;; True three-way selection: an unchanged source datom is
                     ;; not an addition merely because TARGET edited or deleted
                     ;; it. Entity ids for all base datoms are stable across
                     ;; descendants; branch-created entities never occur here.
                     (or (nil? base-db)
                         (empty? (d/datoms base-db :eavt e a v))))
        diff       (->> (d/datoms source-db :eavt)
                        (keep (fn [dt]
                                (let [e (:e dt) a (:a dt) v (:v dt)]
                                  (when (and (not= :db/txInstant a)
                                             ;; NEVER merge SCHEMA as data: a `:db/*`
                                             ;; datom is an attribute/enum definition.
                                             ;; Re-transacting it as flat data upserts a
                                             ;; schema entity to two existing ones and
                                             ;; aborts the whole merge. Schema is
                                             ;; installed at startup and shared by parent
                                             ;; + fork — leave it alone.
                                             (not= "db" (namespace a))
                                             (changed? e a v)
                                             (not (present? e a v)))
                                    [e a v])))))]
    (vec (for [[e a v] diff
               :let  [subj (addr e)]
               ;; A source entity may gain another unique identity after the
               ;; fork. Preserve that one-sided addition. Only omit the add when
               ;; TARGET already assigns the value somewhere: `present?` removed
               ;; the same-entity case, while `compute-conflicts` exposes the
               ;; remaining split-entity case for explicit resolution.
               :when (not (and (vector? subj) (unique a) (in-target? a v)))
               :let  [val (if (and (ref a) (integer? v)) (addr v) v)]]
           [:db/add subj a val]))))

(defn- compute-merge-retractions
  "3-way retraction tx-data: datoms present in BASE and still in TARGET but
   ABSENT from SOURCE — the branch deleted them, so the merge must too.
   Inherently delete-vs-edit safe by construction: if TARGET changed the
   datom since base (edited value, re-parented ref, …), the base datom is
   no longer 'still in target' and nothing is retracted — the edit wins
   and the divergence surfaces via `conflicts`, not silent data loss.
   Subjects and ref values are addressed by :db.unique/identity lookup in
   TARGET, or by their shared raw eid when they are anonymous entities inherited
   from BASE. Branch-created anonymous entities cannot occur in BASE."
  [base-db source-db target-db]
  (let [{:keys [unique ref]} (schema-attrs base-db)
        ident-of (fn [db e]
                   (let [ent (d/entity db e)]
                     (some (fn [ua] (when-some [uv (get ent ua)] [ua uv])) unique)))
        find-e   (fn [db [ua uv]]
                   (d/q '[:find ?e . :in $ ?ua ?uv :where [?e ?ua ?uv]] db ua uv))
        source-e? (fn [e] (boolean (seq (d/datoms source-db :eavt e))))
        target-e? (fn [e] (boolean (seq (d/datoms target-db :eavt e))))
        source-address (fn [e]
                         (or (some->> (ident-of base-db e) (find-e source-db))
                             (when (source-e? e) e)))
        source-present?
        (fn [e a v]
          (when-let [se (source-address e)]
            (let [sv (if (and (ref a) (integer? v))
                       (source-address v)
                       v)]
              (and (some? sv)
                   (seq (d/datoms source-db :eavt se a sv))))))
        deleted  (->> (d/datoms base-db :eavt)
                      (keep (fn [dt]
                              (let [e (:e dt) a (:a dt) v (:v dt)]
                                (when (and (not= :db/txInstant a)
                                           (not= "db" (namespace a))
                                           (not (source-present? e a v)))
                                  [e a v])))))]
    (vec (for [[e a v] deleted
               :let  [subj-id (ident-of base-db e)
                      te      (or (some->> subj-id (find-e target-db))
                                  (when (target-e? e) e))]
               :when te
               :let  [tv (if (and (ref a) (integer? v))
                           (or (some->> (ident-of base-db v) (find-e target-db))
                               (when (target-e? v) v))
                           v)]
               :when (some? tv)
               ;; still present in TARGET? (else nothing to retract — covers
               ;; the target-edited-since-base case)
               :when (some #(= tv (:v %)) (d/datoms target-db :eavt te a))]
           [:db/retract te a tv]))))

;; ============================================================
;; History traversal (synchronous, bounded)
;; ============================================================

(defn- walk-history
  "Walk commit graph from starting refs, collecting snapshot-ids.
   Returns vector of commit-id STRINGS in traversal order.

   The queue/visited carry the raw refs (branch keyword or commit UUID) so
   konserve `k/get` can load each node, but the RESULT is stringified — the
   protocol's snapshot-ids are strings (`snapshot-id` returns `(str …)`), and
   every consumer (`ancestors`, `ancestor?`, `common-ancestor`, `commit-graph`)
   compares with `(str …)`. Returning UUID objects here silently broke all of
   them (a UUID never equals its own string in a set lookup → common-ancestor
   always returned nil)."
  [store start-refs {:keys [limit] :or {limit 100}}]
  (loop [queue (vec start-refs)
         visited #{}
         result []]
    (if (or (empty? queue)
            (and limit (>= (count result) limit)))
      result
      (let [[current & rest] queue]
        (if (visited current)
          (recur (vec rest) visited result)
          (if-let [raw-db (k/get store current nil {:sync? true})]
            (let [parents (parent-ids-of raw-db)]
              (recur (into (vec rest) parents)
                     (conj visited current)
                     (conj result (str (commit-id-of raw-db)))))
            (recur (vec rest) (conj visited current) result)))))))

;; ============================================================
;; DatahikeSystem record
;; ============================================================

;; ============================================================
;; DatahikeOverlay — branch-based isolated workspace (Overlayable)
;; ============================================================
;; datahike is VERSIONED (not convergent), so its overlay is a native BRANCH
;; fork: `overlay` branches+checks-out a fresh overlay branch (the writable
;; system); `merge-down!` 3-way-merges it back into the parent branch; `discard!`
;; deletes the fork branch. (`local-writes` holds the forked system in an atom so
;; the uniform `overlay-system` accessor works across all overlay kinds.)

;; `mode` is always :frozen — a versioned store can't cheaply do `:following`
;; (live join); a `:following` request degrades to :frozen + manual `advance!`.
(defrecord DatahikeOverlay [parent local-writes fork-branch parent-branch mode]
  p/Overlayable
  (base-ref [_] (p/snapshot-id parent))
  (peek-parent [_] parent)
  (peek-parent [_ _] parent)
  (overlay-writes [_] (p/diff parent (p/snapshot-id parent) (p/snapshot-id @local-writes)))

  ;; These chain the now-async branch!/checkout/merge!/delete-branch! — so they run
  ;; under async+sync and `await` each step (value on JVM, CPS on cljs). The composite
  ;; already `await`s merge-down!/advance! (composite.cljc), so a CPS return is expected.
  (advance! [ov] (p/advance! ov nil))
  (advance! [ov opts]                               ; :following — merge parent INTO the fork
    (async+sync (sync?* opts)
                (async
                 (reset! local-writes (await (p/merge! @local-writes parent-branch)))
                 ov)))

  ;; 3-way merge the fork branch back into the parent branch → merged parent.
  (merge-down! [ov] (p/merge-down! ov nil))
  (merge-down! [_ opts]
    (async+sync (sync?* opts)
                (async
                 (let [pco (await (p/checkout parent parent-branch))]
                   (await (p/merge! pco fork-branch))))))

  (discard! [ov] (p/discard! ov nil))
  (discard! [_ opts]
    (async+sync (sync?* opts)
                (async (await (p/delete-branch! parent fork-branch)) nil))))

;; `checkout` below builds a fresh DatahikeSystem via the record's own constructor;
;; forward-declare it so cljs's single-pass analyzer doesn't warn :undeclared-var.
(declare ->DatahikeSystem)

(defrecord DatahikeSystem [conn system-name]
  p/SystemIdentity
  (system-id [_]
    (or system-name
        (str "datahike:" (get-in @conn [:config :store :id]))))
  (system-type [_] :datahike)
  (capabilities [_]
    (t/->Capabilities true true true true true false true false false))

  p/Snapshotable
  (snapshot-id [_]
    (str (commit-id-of (db-of conn))))

  (parent-ids [_]
    (let [parents (parent-ids-of (db-of conn))]
      (set (map str parents))))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (let [store (store-of conn)
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (dv/commit-as-db store uuid)))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [store (store-of conn)
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (when-let [raw-db (k/get store uuid nil {:sync? true})]
        (snapshot-meta-of-stored raw-db))))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (let [store (store-of conn)]
      (k/get store :branches nil {:sync? true})))

  (current-branch [_]
    (branch-of conn))

  ;; branch! / delete-branch! are konserve-DIRECT (`dv/*` default `:sync? true`); on a
  ;; genuinely-async backend `:sync? true` would throw, so thread the platform/opts
  ;; `:sync?` and bridge via kbridge → value on JVM, await-able CPS on cljs.
  (branch! [this name] (p/branch! this name (branch-of conn) nil))
  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from opts]
    (let [s? (sync?* opts)]
      (async+sync s?
                  (async
                   (await (ca/sync-or-cps (dv/branch! conn from name {:sync? s?}) {:sync? s?}))
                   this))))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [_ name opts]
    (let [s? (sync?* opts)]
      (async+sync s?
                  (async
                   (await (ca/sync-or-cps (dv/delete-branch! conn name {:sync? s?}) {:sync? s?}))))))

  ;; checkout reconnects on the branch — `d/connect` is `:sync?`-aware (value on
  ;; JVM, channel on cljs); bridge + yield the branch-scoped system.
  (checkout [this name] (p/checkout this name nil))
  (checkout [this name opts]
    (let [s? (sync?* opts)
          branch-cfg (assoc (:config @conn) :branch name)]
      (async+sync s?
                  (async
                   (->DatahikeSystem (await (ca/sync-or-cps (d/connect branch-cfg {:sync? s?}) {:sync? s?}))
                                     system-name)))))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [store (store-of conn)
          branch (branch-of conn)]
      (walk-history store [branch] opts)))

  ;; Ref normalization for ancestry ops: branch KEYWORDS pass through
  ;; (walk-history and branch-as-db take them directly — parse-uuid'ing a
  ;; keyword yields nil and silently broke every keyword-ref call);
  ;; uuid-strings parse; uuids pass.
  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [store (store-of conn)
          ref (cond (keyword? snap-id) snap-id
                    (uuid? snap-id) snap-id
                    :else (parse-uuid (str snap-id)))]
      (walk-history store [ref] {:limit nil})))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [this a b _opts]
    (let [norm (fn [x] (cond (keyword? x) x (uuid? x) x
                             :else (or (parse-uuid (str x)) x)))
          ancestors-of-b (set (p/ancestors this (norm b)))]
      (contains? ancestors-of-b (str (norm a)))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (let [store (store-of conn)
          norm (fn [x] (cond (keyword? x) x (uuid? x) x
                             :else (or (parse-uuid (str x)) x)))
          load-stored (fn [r]
                        (try (let [ref (cond (keyword? r) r
                                             (uuid? r) r
                                             :else (some->> r str parse-uuid))]
                               (when ref (k/get store ref nil {:sync? true})))
                             (catch #?(:clj Exception :cljs :default) _ nil)))
          ancestors-a (set (walk-history store [(norm a)] {:limit nil}))]
      (loop [queue [(norm b)]
             visited #{}]
        (when (seq queue)
          (let [[current & rest] queue]
            (if (visited current)
              (recur (vec rest) visited)
              (if-let [raw-db (load-stored current)]
                ;; test the COMMIT of the loaded ref itself (a branch head or
                ;; parent commit) — the old loop only tested raw queue entries,
                ;; so a fast-forward base (b's own head) was never found.
                (let [cid (str (commit-id-of raw-db))]
                  (if (ancestors-a cid)
                    cid
                    (recur (into (vec rest) (parent-ids-of raw-db))
                           (conj visited current))))
                (recur (vec rest) (conj visited current)))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [this _opts]
    (let [store (store-of conn)
          branches (p/branches this)
          all-ids (walk-history store (vec branches) {:limit nil})]
      {:nodes (into {}
                    (for [id all-ids
                          :let [uuid (parse-uuid id)
                                raw-db (when uuid (k/get store uuid nil {:sync? true}))]
                          :when raw-db]
                      [id {:parent-ids (set (map str (parent-ids-of raw-db)))
                           :meta (snapshot-meta-of-stored raw-db)}]))
       :branches (into {}
                       (for [b branches]
                         [b (when-let [raw-db (k/get store b nil {:sync? true})]
                              (str (commit-id-of raw-db)))]))
       :roots (set (filter
                    (fn [id]
                      (let [uuid (parse-uuid id)
                            raw-db (when uuid (k/get store uuid nil {:sync? true}))]
                        (or (nil? raw-db) (empty? (parent-ids-of raw-db)))))
                    all-ids))}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (p/snapshot-meta this snap-id))

  p/GarbageCollectable
  (gc-roots [_]
    (let [store (store-of conn)
          branches (k/get store :branches nil {:sync? true})]
      (->> branches
           (map (fn [branch]
                  (when-let [raw-db (k/get store branch nil {:sync? true})]
                    (str (commit-id-of raw-db)))))
           (remove nil?)
           set)))

  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [_ _snapshot-ids opts]
    ;; Reclaim unreachable datahike index blobs (orphaned hitchhiker-tree nodes
    ;; left behind by every transaction). Datahike computes its OWN reachability
    ;; — it always keeps every branch head + that head's history — so we ignore
    ;; the coordinator's snapshot-ids and just forward the retention cutoff.
    ;;   :remove-before <java.util.Date> — also collapse snapshots committed
    ;;     before this instant. Default (Date. 0) = epoch = keep ALL history,
    ;;     deleting only the orphaned rewrite garbage (safe to run anytime).
    ;;   :dry-run?      — report nothing reclaimed without touching storage.
    ;; `d/gc-storage` is datahike's async reclamation (writer → throwable-promise).
    ;; GC is ASYNC-ONLY (yggdrasil never blocks): on BOTH platforms we yield a
    ;; partial-cps CPS over the reclamation channel — await-able like every other
    ;; adapter op; a caller that wants to wait blocks at its OWN boundary.
    ;; Returns {:system-id … :reclaimed <key-count>} (or :dry-run?).
    (let [opts (t/async-gc-opts "datahike/gc-sweep!" opts)]
      (async
       (if (:dry-run? opts)
         {:system-id system-name :dry-run? true}
         (let [remove-before (or (:remove-before opts) (#?(:clj java.util.Date. :cljs js/Date.) 0))
               removed (await (ca/chan->cps (d/gc-storage conn remove-before)))]
           {:system-id system-name :reclaimed (reclaimed-count removed)})))))

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this source opts]
    (let [store (store-of conn)
          source-branch (if (keyword? source) source nil)
          parents (if (keyword? source)
                    #{source}
                    #{(if (uuid? source) source (parse-uuid (str source)))})
          tx-data (or (:tx-data opts)
                      (when source-branch
                        (let [source-db (dv/branch-as-db store source-branch)
                              target-db (db-of conn)
                              ;; 3-way: merge-base enables RETRACTION propagation
                              ;; (branch deletions land on target). Base
                              ;; unavailable (GC'd / no common ancestor) →
                              ;; additions-only, as before — logged upstream by
                              ;; consumers via conflicts' baseless fallback.
                              base-id (try (p/common-ancestor this source-branch
                                                              (p/current-branch this))
                                           (catch #?(:clj Exception :cljs :default) _ nil))
                              ;; snapshot-ids are STRINGS (walk-history); resolve-db
                              ;; wants the UUID
                              base-db (some->> base-id str parse-uuid (resolve-db store))]
                          ;; identity-keyed (sibling-safe), not raw [:db/add e a v]
                          (into (compute-merge-tx source-db target-db base-db)
                                (when base-db
                                  (compute-merge-retractions base-db source-db target-db)))))
                      [])]
      ;; merge routes through the datahike WRITER (genuinely async — a go-loop
      ;; transactor). On the JVM datahike's OWN sync API derefs the writer's
      ;; CompletableFuture (`dv/merge!` = `@(merge-db! …)`) — its native blocking-wait,
      ;; not a core.async `<!!`. On cljs the writer hands back a promise-chan (no
      ;; IDeref); bridge it to an await-able partial-cps CPS. Both yield the system.
      #?(:clj  (do (dv/merge! conn parents tx-data (:tx-meta opts))
                   this)
         :cljs (async
                (await (ca/chan->cps (dv/merge-async! conn parents tx-data (:tx-meta opts))))
                this))))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [this a b _opts]
    ;; 3-way: conflicts between `a` (ours) and `b` (theirs) relative to their
    ;; merge-base. Identity-keyed additions union (not conflicts); only a
    ;; cardinality-one attr that BOTH sides changed differently is a conflict.
    (let [store   (store-of conn)
          db-a    (resolve-db store a)
          db-b    (resolve-db store b)
          base-id (p/common-ancestor this a b)
          db-base (when base-id (resolve-db store base-id))]
      (cond
        ;; merge-base available → precise 3-way conflict detection.
        (and db-a db-b db-base) (compute-conflicts db-base db-a db-b)
        ;; base UNAVAILABLE (GC'd by retention, or no common ancestor) but both
        ;; heads resolve → conservative 2-way fallback, NEVER a silent `[]` that
        ;; would let the merge gate blind-merge a divergent stale fork.
        (and db-a db-b)         (compute-conflicts-baseless db-a db-b)
        ;; a head itself unresolvable → nothing to compare.
        :else                   [])))

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    (let [store (store-of conn)
          resolve-db (fn [x]
                       (cond
                         (keyword? x) (dv/branch-as-db store x)
                         (uuid? x)    (dv/commit-as-db store x)
                         :else        (when-let [u (parse-uuid (str x))]
                                        (dv/commit-as-db store u))))
          db-a (resolve-db a)
          db-b (resolve-db b)]
      (if (and db-a db-b)
        (let [added   (compute-branch-diff db-b db-a)
              removed (compute-branch-diff db-a db-b)]
          (t/->DatahikeDiff
           a b added removed
           {:added-datoms (count added)
            :removed-datoms (count removed)
            :entities-touched (count (into (set (map second added))
                                           (map second removed)))}))
        (t/->DiffError a b "Could not resolve branch/snapshot"))))

  p/Overlayable
  ;; native branch fork: branch+checkout a fresh overlay branch as the writable
  ;; system; `merge-down!` 3-way-merges it back, `discard!` deletes it.
  (overlay [this opts]
    (let [s?      (sync?* opts)
          pbranch (p/current-branch this)
          fbranch (keyword (str "overlay-" (random-uuid)))]
      ;; branch! + checkout are now async (value JVM / CPS cljs) — await each.
      (async+sync s?
                  (async
                   (await (p/branch! this fbranch))
                   (let [forked (await (p/checkout this fbranch))]
           ;; :following degrades to :frozen for a versioned store (honest fallback).
                     (->DatahikeOverlay this (atom forked) fbranch pbranch :frozen)))))))

;; ============================================================
;; Constructor
;; ============================================================

(defn create
  "Create a Datahike adapter from an existing connection.

   (create conn)
   (create conn {:system-name \"my-datahike-db\"})"
  ([conn] (create conn {}))
  ([conn opts]
   (->DatahikeSystem conn (:system-name opts))))

;; ============================================================
;; Native commit hook via d/listen
;; ============================================================

(defmethod hooks/install-commit-hook! :datahike
  [_workspace system on-commit-fn]
  (let [conn (:conn system)
        listener-key (keyword (str "yggdrasil-" (p/system-id system)))]
    (d/listen conn listener-key
              (fn [tx-report]
                (when-let [db (:db-after tx-report)]
                  (when-let [cid (commit-id-of db)]
                    (let [snap-id (str cid)
                          branch (name (get-in db [:config :branch]))]
                      (on-commit-fn {:type :commit
                                     :snapshot-id snap-id
                                     :branch branch
                                     :timestamp (t/now-ms)}))))))
    listener-key))

(defmethod hooks/remove-commit-hook! :datahike
  [_workspace system hook-id]
  (d/unlisten (:conn system) hook-id))

;; Register Datahike with the system value codec — external-ref flavor, DON'T overload
;; datahike. Datahike already owns the DB's OWN serialization (its fused root + Datom
;; fressian handlers + store-id scope registry). So the value codec serializes only the
;; connection CONFIG (a reference) and reconstruct RECONNECTS via `d/connect` — the DB
;; data is never inlined here. (Same shape as git; resolve-storage unused.)
;; JVM-only: fressian (org.fressian) is the wire/at-rest codec; cljs peers carry the
;; system as a live ygg-signal value, not a serialized blob.
#?(:clj
   (yf/register-system!
    :datahike DatahikeSystem
    (fn [{:keys [conn system-name]}]
      {:config (:config @conn) :system-name system-name})
    (fn [blob _storage _opts]
      (create (d/connect (:config blob)) {:system-name (:system-name blob)}))))
