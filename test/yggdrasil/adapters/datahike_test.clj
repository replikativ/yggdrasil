(ns yggdrasil.adapters.datahike-test
  "Graphable regression tests for the Datahike adapter.

   These methods were previously untested, which let a type bug survive:
   walk-history collected commit-ids as UUID objects while every consumer
   (ancestor?, common-ancestor, commit-graph) compared with (str …), so a UUID
   never matched its own string in a set lookup and common-ancestor always
   returned nil — silently breaking fork merge-base derivation."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [datahike.writing :as dw]
            [yggdrasil.adapters.datahike :as dha]
            [yggdrasil.convergent.overlay :as ovl]
            [yggdrasil.protocols :as p]))

(def ^:dynamic *conn* nil)
(def ^:dynamic *cfg* nil)

(defn with-mem-db [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history?      true
             :schema-flexibility :read}]
    (d/create-database cfg)
    (binding [*cfg* cfg *conn* (d/connect cfg)]
      (try (f)
           (finally (d/release *conn*) (d/delete-database cfg))))))

(use-fixtures :each with-mem-db)

(deftest history-returns-string-snapshot-ids
  (testing "history/ancestors return STRING snapshot-ids (the protocol type)"
    (let [sys (dha/create *conn* {:system-name "test-db"})]
      (d/transact *conn* [{:note/text "a"}])
      (d/transact *conn* [{:note/text "b"}])
      (let [hist (p/history sys)]
        (is (seq hist) "history is non-empty")
        (is (every? string? hist) "every history entry is a string snapshot-id")
        (is (string? (p/snapshot-id sys)))))))

(deftest graph-metadata-does-not-materialize-writable-dbs
  (testing "history and branch heads are read directly from stored metadata"
    (let [sys (dha/create *conn* {:system-name "test-db"})]
      (d/transact *conn* [{:note/text "a"}])
      (d/transact *conn* [{:note/text "b"}])
      ;; Historical snapshots may carry secondary indexes whose live writer is
      ;; already open on the connection. Reconstructing them through stored->db
      ;; tries to open a second writer and fails on its lock; graph operations
      ;; only need :meta and must never materialize those snapshots at all.
      (with-redefs [dw/stored->db
                    (fn [& _]
                      (throw (ex-info "writable DB reconstruction is forbidden" {})))]
        (is (seq (p/history sys)))
        (is (= #{(p/snapshot-id sys)} (p/gc-roots sys)))
        (is (= (p/snapshot-id sys) (get-in (p/commit-graph sys) [:branches :db])))))))

(deftest sibling-merge-unions-by-identity
  (testing "two SIBLING branches each add an entity from the same base; merging
            both UNIONS them (no entity-id collision clobbering one) — the
            Mannheim/San-Francisco data-loss scenario"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :note/id   :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :note/text :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
      (d/transact *conn* [{:note/id "base" :note/text "base"}])
      (p/branch! sys :sib-a)
      (p/branch! sys :sib-b)
      (let [asys (p/checkout sys :sib-a)
            bsys (p/checkout sys :sib-b)]
        ;; both forks allocate "the next entity-id after base" for their note
        (d/transact (:conn asys) [{:note/id "A" :note/text "mannheim"}])
        (d/transact (:conn bsys) [{:note/id "B" :note/text "san-francisco"}])
        (p/merge! sys :sib-a)
        (p/merge! sys :sib-b)
        (let [ids (set (d/q '[:find [?id ...] :where [_ :note/id ?id]] @*conn*))]
          (is (contains? ids "A") "sibling A survived")
          (is (contains? ids "B") "sibling B survived — NOT clobbered by A's merge")
          (is (= #{"base" "A" "B"} ids) "clean union of both siblings"))))))

(deftest merge-resolves-refs-among-co-created-entities
  (testing "a fork adds a NEW entity AND another NEW entity that refs it; merging
            both must resolve the ref intra-tx via shared tempids — not fail on a
            lookup-ref to an entity being upserted in the same tx (the live
            chat-ctx ← ledger/context merge failure)"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :box/id  :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :item/id :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :item/box :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one}])
      (d/transact *conn* [{:box/id "seed"}])
      (p/branch! sys :feat)
      (let [fsys (p/checkout sys :feat)]
        ;; both NEW in parent: a box and an item pointing at it (like a fork's new
        ;; chat-context + the ledger rows that reference it)
        (d/transact (:conn fsys) [{:box/id "b1"}])
        (d/transact (:conn fsys) [{:item/id "i1" :item/box [:box/id "b1"]}])
        (p/merge! sys :feat)
        (let [db @*conn*]
          (is (= #{"seed" "b1"} (set (d/q '[:find [?id ...] :where [_ :box/id ?id]] db))) "box merged")
          (is (= #{"i1"} (set (d/q '[:find [?id ...] :where [_ :item/id ?id]] db))) "item merged")
          (is (= "b1" (ffirst (d/q '[:find ?bid :where [?i :item/id "i1"] [?i :item/box ?b] [?b :box/id ?bid]] db)))
              "item's ref resolved to the co-created box"))))))

(def document-schema
  [{:db/ident :document/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :document/lines
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}
   {:db/ident :line/text
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :line/next
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :line/tag
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(defn- line-texts [db document-id]
  (set (d/q '[:find [?text ...]
              :in $ ?document-id
              :where
              [?d :document/id ?document-id]
              [?d :document/lines ?line]
              [?line :line/text ?text]]
            db document-id)))

(defn- line-eid [db document-id text]
  (d/q '[:find ?line .
         :in $ ?document-id ?text
         :where
         [?d :document/id ?document-id]
         [?d :document/lines ?line]
         [?line :line/text ?text]]
       db document-id text))

(deftest merge-does-not-replay-inherited-anonymous-audit-rows
  (testing "an unchanged child cannot append fresh copies of sealed parent history"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact
       *conn*
       [{:db/ident :audit.transaction/id
         :db/valueType :db.type/uuid
         :db/cardinality :db.cardinality/one
         :db/unique :db.unique/identity}
        {:db/ident :audit.posting/transaction
         :db/valueType :db.type/ref
         :db/cardinality :db.cardinality/one}
        {:db/ident :audit.posting/amount
         :db/valueType :db.type/bigdec
         :db/cardinality :db.cardinality/one}
        {:db/ident :note/id
         :db/valueType :db.type/string
         :db/cardinality :db.cardinality/one
         :db/unique :db.unique/identity}])
      (let [transaction-id (random-uuid)]
        ;; Like Kontor, the transaction is identified while its balanced
        ;; postings are deliberately anonymous immutable history.
        (d/transact *conn*
                    [{:audit.transaction/id transaction-id}
                     {:audit.posting/transaction
                      [:audit.transaction/id transaction-id]
                      :audit.posting/amount 1M}
                     {:audit.posting/transaction
                      [:audit.transaction/id transaction-id]
                      :audit.posting/amount -1M}])
        (let [base-postings
              (set (d/q '[:find [?posting ...]
                          :in $ ?transaction-id
                          :where
                          [?tx :audit.transaction/id ?transaction-id]
                          [?posting :audit.posting/transaction ?tx]]
                        @*conn* transaction-id))]
          (p/branch! sys :child)
          (let [child (p/checkout sys :child)]
            ;; Exercise a real merge transaction while leaving the inherited
            ;; audit rows untouched in the source.
            (d/transact (:conn child) [{:note/id "child-output"}])
            (p/merge! sys :child)
            (let [merged-postings
                  (set (d/q '[:find [?posting ...]
                              :in $ ?transaction-id
                              :where
                              [?tx :audit.transaction/id ?transaction-id]
                              [?posting :audit.posting/transaction ?tx]]
                            @*conn* transaction-id))]
              (is (= base-postings merged-postings)
                  "merge preserves the exact inherited anonymous posting identities")
              (is (= #{1M -1M}
                     (set (d/q '[:find [?amount ...]
                                 :in $ ?transaction-id
                                 :where
                                 [?tx :audit.transaction/id ?transaction-id]
                                 [?posting :audit.posting/transaction ?tx]
                                 [?posting :audit.posting/amount ?amount]]
                               @*conn* transaction-id)))
                  "no duplicate posting history is appended"))))))))

(deftest merge-preserves-and-retracts-base-anonymous-components
  (testing "inherited anonymous components use their shared base eid"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* document-schema)
      (d/transact *conn* [{:document/id "sealed"
                           :document/lines [{:line/text "debit"}
                                            {:line/text "credit"}]}])
      (p/branch! sys :feature)
      (let [fsys (p/checkout sys :feature)]
        ;; Diverge both heads without changing either inherited component.
        (d/transact (:conn fsys) [{:document/id "fork"}])
        (d/transact *conn* [{:document/id "trunk"}])
        (p/merge! sys :feature)
        (is (= #{"debit" "credit"} (line-texts @*conn* "sealed"))
            "unchanged components are neither duplicated nor lost"))
      (p/branch! sys :delete-line)
      (let [delete-sys (p/checkout sys :delete-line)
            line (line-eid @(:conn delete-sys) "sealed" "debit")]
        (d/transact (:conn delete-sys) [[:db/retractEntity line]])
        (p/merge! sys :delete-line)
        (is (= #{"credit"} (line-texts @*conn* "sealed"))
            "source deletion of an inherited component propagates")))))

(deftest merge-does-not-clobber-target-only-anonymous-changes
  (testing "unchanged source history cannot overwrite or resurrect target data"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* document-schema)
      (d/transact *conn* [{:document/id "edited"
                           :document/lines [{:line/text "base-edit"}]}
                          {:document/id "deleted"
                           :document/lines [{:line/text "base-delete"}]}])
      (p/branch! sys :no-op-source)
      (let [source (p/checkout sys :no-op-source)
            edited-line (line-eid @*conn* "edited" "base-edit")
            deleted-line (line-eid @*conn* "deleted" "base-delete")]
        (d/transact (:conn source) [{:document/id "source-unrelated"}])
        (d/transact *conn* [[:db/add edited-line :line/text "target-edit"]
                            [:db/retractEntity deleted-line]])
        (p/merge! sys :no-op-source)
        (is (= #{"target-edit"} (line-texts @*conn* "edited"))
            "target-only edit remains authoritative")
        (is (empty? (line-texts @*conn* "deleted"))
            "target-only deletion is not resurrected")))))

(deftest merge-applies-source-only-anonymous-edit
  (testing "a source change to an inherited anonymous entity lands on target"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* document-schema)
      (d/transact *conn* [{:document/id "doc"
                           :document/lines [{:line/text "base"}]}])
      (p/branch! sys :source-edit)
      (let [source (p/checkout sys :source-edit)
            line (line-eid @(:conn source) "doc" "base")]
        (d/transact (:conn source) [[:db/add line :line/text "source-edit"]])
        (p/merge! sys :source-edit)
        (is (= #{"source-edit"} (line-texts @*conn* "doc")))))))

(deftest conflicts-cover-anonymous-edits-and-deletions
  (testing "base eid supplies semantic identity for anonymous 3-way conflicts"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* document-schema)
      (d/transact *conn* [{:document/id "edited"
                           :document/lines [{:line/text "base-edit"}]}
                          {:document/id "deleted"
                           :document/lines [{:line/text "base-delete"}]}])
      (let [edit-eid (line-eid @*conn* "edited" "base-edit")
            delete-eid (line-eid @*conn* "deleted" "base-delete")]
        (p/branch! sys :ours)
        (p/branch! sys :theirs)
        (let [ours (p/checkout sys :ours)
              theirs (p/checkout sys :theirs)]
          (d/transact (:conn ours) [[:db/add edit-eid :line/text "ours"]
                                    [:db/retractEntity delete-eid]])
          (d/transact (:conn theirs) [[:db/add edit-eid :line/text "theirs"]
                                      [:db/add delete-eid :line/text "theirs-edit"]])
          (let [conflicts (p/conflicts ours (p/snapshot-id ours)
                                       (p/snapshot-id theirs))
                by-entity (group-by :entity conflicts)]
            (is (= #{[:yggdrasil/base-eid edit-eid]
                     [:yggdrasil/base-eid delete-eid]}
                   (set (keys by-entity))))
            (is (= #{["base-edit" "ours" "theirs"]
                     ["base-delete" nil "theirs-edit"]}
                   (set (map (juxt :base :ours :theirs) conflicts))))))))))

(deftest entity-tombstone-conflicts-with-new-attributes
  (testing "delete-vs-add conflicts for identified and inherited anonymous entities"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn*
                  (into document-schema
                        [{:db/ident :note/id
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one
                          :db/unique :db.unique/identity}
                         {:db/ident :note/text
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :note/tag
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}]))
      (d/transact *conn*
                  [{:note/id "conflict" :note/text "base"}
                   {:note/id "one-sided-add" :note/text "base"}
                   {:note/id "pure-delete" :note/text "base"}
                   {:document/id "doc"
                    :document/lines [{:line/text "conflict"}
                                     {:line/text "one-sided-add"}
                                     {:line/text "pure-delete"}]}])
      (let [identified-conflict
            (d/q '[:find ?e . :where [?e :note/id "conflict"]] @*conn*)
            identified-delete
            (d/q '[:find ?e . :where [?e :note/id "pure-delete"]] @*conn*)
            anonymous-conflict (line-eid @*conn* "doc" "conflict")
            anonymous-delete (line-eid @*conn* "doc" "pure-delete")]
        (p/branch! sys :deleting)
        (p/branch! sys :modifying)
        (let [deleting (p/checkout sys :deleting)
              modifying (p/checkout sys :modifying)]
          (d/transact (:conn deleting)
                      [[:db/retractEntity identified-conflict]
                       [:db/retractEntity identified-delete]
                       [:db/retractEntity anonymous-conflict]
                       [:db/retractEntity anonymous-delete]
                       {:note/id "one-sided-add" :note/tag "ours-only"}
                       [:db/add (line-eid @(:conn deleting) "doc" "one-sided-add")
                        :line/tag "ours-only"]])
          (d/transact (:conn modifying)
                      [{:note/id "conflict" :note/tag "theirs"}
                       [:db/add anonymous-conflict :line/tag "theirs"]])
          (let [conflicts (p/conflicts deleting (p/snapshot-id deleting)
                                       (p/snapshot-id modifying))]
            (is (= #{[[:note/id "conflict"] :note/tag nil nil "theirs"]
                     [[:yggdrasil/base-eid anonymous-conflict]
                      :line/tag nil nil "theirs"]}
                   (set (map (juxt :entity :attr :base :ours :theirs)
                             conflicts)))
                "new survivor attributes conflict with identified and anonymous tombstones")
            (is (not-any? #(contains? #{[:note/id "one-sided-add"]
                                        [:note/id "pure-delete"]
                                        [:yggdrasil/base-eid anonymous-delete]}
                                      (:entity %))
                          conflicts)
                "ordinary one-sided additions and pure deletions stay conflict-free")))))))

(deftest conflicts-normalize-refs-between-base-anonymous-entities
  (testing "anonymous ref values compare by shared base identity, not Entity object"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* document-schema)
      (d/transact *conn* [{:document/id "doc"
                           :document/lines [{:line/text "a"}
                                            {:line/text "b"}
                                            {:line/text "c"}]}])
      (let [a (line-eid @*conn* "doc" "a")
            b (line-eid @*conn* "doc" "b")
            c (line-eid @*conn* "doc" "c")]
        (d/transact *conn* [[:db/add a :line/next b]])
        (p/branch! sys :ours-ref)
        (p/branch! sys :theirs-ref)
        (let [ours (p/checkout sys :ours-ref)
              theirs (p/checkout sys :theirs-ref)]
          (d/transact (:conn ours) [[:db/add a :line/next c]])
          (d/transact (:conn theirs) [[:db/retract a :line/next b]])
          (let [conflict (->> (p/conflicts ours (p/snapshot-id ours)
                                           (p/snapshot-id theirs))
                              (filter #(= :line/next (:attr %)))
                              first)]
            (is (= [:yggdrasil/base-eid a] (:entity conflict)))
            (is (= [:yggdrasil/base-eid b] (:base conflict)))
            (is (= [:yggdrasil/base-eid c] (:ours conflict)))
            (is (nil? (:theirs conflict)))))))))

(deftest sibling-created-anonymous-components-do-not-collide
  (testing "equal branch-local numeric eids never identify new anonymous values"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* document-schema)
      (d/transact *conn* [{:document/id "doc"}])
      (p/branch! sys :left)
      (p/branch! sys :right)
      (let [left (p/checkout sys :left)
            right (p/checkout sys :right)]
        (d/transact (:conn left) [{:document/id "doc"
                                   :document/lines [{:line/text "left"}]}])
        (d/transact (:conn right) [{:document/id "doc"
                                    :document/lines [{:line/text "right"}]}])
        (p/merge! sys :left)
        (p/merge! sys :right)
        (is (= #{"left" "right"} (line-texts @*conn* "doc"))
            "both sibling-created components survive")))))

(deftest merge-ignores-schema-attrs-added-in-fork
  (testing "a fork that registers a new SCHEMA attribute (a tool input schema) must
            not abort the merge — :db/* datoms are schema, not data, and upserting
            them as data resolves a tempid to two schema entities and rolls back"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :note/id :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :note/text :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
      (d/transact *conn* [{:note/id "base" :note/text "base"}])
      (p/branch! sys :feat)
      (let [fsys (p/checkout sys :feat)]
        ;; the fork registers a NEW attribute AND writes a note (data)
        (d/transact (:conn fsys) [{:db/ident :note/tag :db/valueType :db.type/string
                                   :db/cardinality :db.cardinality/one}])
        (d/transact (:conn fsys) [{:note/id "n1" :note/text "hi" :note/tag "x"}])
        (p/merge! sys :feat)            ; must not throw
        (let [db @*conn*]
          (is (= #{"base" "n1"} (set (d/q '[:find [?id ...] :where [_ :note/id ?id]] db)))
              "the data note merged despite the fork-added schema attr"))))))

(deftest conflicts-detects-3way-field-clash
  (testing "two branches changing the SAME cardinality-one attr to DIFFERENT
            values = a conflict; one-sided changes are not"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :note/id   :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :note/text :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
      (d/transact *conn* [{:note/id "n" :note/text "base"} {:note/id "m" :note/text "m-base"}])
      (p/branch! sys :ours)
      (p/branch! sys :theirs)
      (let [osys (p/checkout sys :ours)
            tsys (p/checkout sys :theirs)]
        ;; both change n's text differently (CONFLICT); only ours changes m (no conflict)
        (d/transact (:conn osys) [{:note/id "n" :note/text "ours-val"}
                                  {:note/id "m" :note/text "m-ours"}])
        (d/transact (:conn tsys) [{:note/id "n" :note/text "theirs-val"}])
        (let [confs (p/conflicts osys (p/snapshot-id osys) (p/snapshot-id tsys))]
          (is (= 1 (count confs)) "exactly one conflict (n's text), not m")
          (let [c (first confs)]
            (is (= [:note/id "n"] (:entity c)))
            (is (= :note/text (:attr c)))
            (is (= "base" (:base c)))
            (is (= "ours-val" (:ours c)))
            (is (= "theirs-val" (:theirs c)))))))))

(deftest identified-entities-use-the-same-three-way-rules
  (testing "target-only changes survive and identity delete-vs-edit conflicts"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :note/id
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db/ident :note/text
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
      (d/transact *conn* [{:note/id "target-only" :note/text "base"}
                          {:note/id "delete-edit" :note/text "base-delete"}])
      (p/branch! sys :identity-no-op)
      (let [source (p/checkout sys :identity-no-op)]
        (d/transact (:conn source) [{:note/id "source-unrelated"}])
        (d/transact *conn* [{:note/id "target-only" :note/text "target"}])
        (p/merge! sys :identity-no-op)
        (is (= "target"
               (d/q '[:find ?text .
                      :where [?e :note/id "target-only"]
                      [?e :note/text ?text]] @*conn*))))
      (p/branch! sys :identity-delete)
      (p/branch! sys :identity-edit)
      (let [deleting (p/checkout sys :identity-delete)
            editing (p/checkout sys :identity-edit)
            eid (d/q '[:find ?e . :where [?e :note/id "delete-edit"]]
                     @(:conn deleting))]
        (d/transact (:conn deleting) [[:db/retractEntity eid]])
        (d/transact (:conn editing) [{:note/id "delete-edit"
                                      :note/text "edited"}])
        (is (= [{:entity [:note/id "delete-edit"]
                 :attr :note/text
                 :base "base-delete"
                 :ours nil
                 :theirs "edited"}]
               (filterv #(= :note/text (:attr %))
                        (p/conflicts deleting (p/snapshot-id deleting)
                                     (p/snapshot-id editing)))))))))

(deftest identified-entity-recreation-is-a-semantic-update
  (testing "recreating one identity at a new eid does not retract that identity"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :note/id
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db/ident :note/text
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
      (d/transact *conn* [{:note/id "n" :note/text "base"}])
      (p/branch! sys :recreate)
      (let [source (p/checkout sys :recreate)
            old-eid (d/q '[:find ?e . :where [?e :note/id "n"]]
                         @(:conn source))]
        (d/transact (:conn source) [[:db/retractEntity old-eid]])
        (d/transact (:conn source) [{:note/id "n" :note/text "recreated"}])
        (p/merge! sys :recreate)
        (is (= #{["n" "recreated"]}
               (d/q '[:find ?id ?text
                      :where
                      [?e :note/id ?id]
                      [?e :note/text ?text]] @*conn*))
            "semantic lookup remains intact after merge")))))

(deftest entity-deletion-conflicts-with-cardinality-many-edit
  (testing "delete-vs-many modification cannot leave an orphaned remainder"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :note/id
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db/ident :note/tags
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/many}])
      (d/transact *conn* [{:note/id "n" :note/tags #{"base"}}])
      (p/branch! sys :delete-note)
      (p/branch! sys :edit-tags)
      (let [deleting (p/checkout sys :delete-note)
            editing (p/checkout sys :edit-tags)
            eid (d/q '[:find ?e . :where [?e :note/id "n"]]
                     @(:conn deleting))]
        (d/transact (:conn deleting) [[:db/retractEntity eid]])
        (d/transact (:conn editing) [[:db/add [:note/id "n"]
                                      :note/tags "added"]])
        (is (= [{:entity [:note/id "n"]
                 :attr :note/tags
                 :base #{"base"}
                 :ours nil
                 :theirs #{"base" "added"}}]
               (filterv #(= :note/tags (:attr %))
                        (p/conflicts deleting (p/snapshot-id deleting)
                                     (p/snapshot-id editing)))))))))

(deftest identity-acquisition-does-not-change-an-inherited-ref
  (testing "base identity wins over a unique identity added in one descendant"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :doc/id
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db/ident :doc/link
                           :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :line/id
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db/ident :line/text
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
      (let [tx (d/transact *conn* [{:db/id "b" :line/text "b"}
                                   {:db/id "c" :line/text "c"}
                                   {:doc/id "d" :doc/link "b"}])
            b (get (:tempids tx) "b")
            c (get (:tempids tx) "c")]
        (p/branch! sys :gain-identity)
        (p/branch! sys :change-ref)
        (let [ours (p/checkout sys :gain-identity)
              theirs (p/checkout sys :change-ref)]
          (d/transact (:conn ours) [[:db/add b :line/id "B"]])
          (d/transact (:conn theirs) [[:db/add [:doc/id "d"] :doc/link c]])
          (is (empty? (filter #(= :doc/link (:attr %))
                              (p/conflicts ours (p/snapshot-id ours)
                                           (p/snapshot-id theirs))))
              "only the ref-changing branch differs from base"))))))

(deftest common-ancestor-resolves-merge-base
  (testing "common-ancestor finds the fork point across a branch + divergence"
    (let [sys (dha/create *conn* {:system-name "test-db"})]
      ;; two commits on the base branch
      (d/transact *conn* [{:note/text "a"}])
      (d/transact *conn* [{:note/text "b"}])
      (let [base-snap (p/snapshot-id sys)]
        ;; branch off, commit on the branch
        (p/branch! sys :feature)
        (let [fsys (p/checkout sys :feature)]
          (d/transact (:conn fsys) [{:note/text "c-on-feature"}])
          ;; advance the base branch independently (so neither head is the other's ancestor)
          (d/transact *conn* [{:note/text "d-on-base"}])
          (let [parent-snap (p/snapshot-id sys)
                fork-snap   (p/snapshot-id fsys)
                ca          (p/common-ancestor fsys parent-snap fork-snap)]
            (is (string? ca) "common-ancestor returns a string snapshot-id, not nil")
            (is (not= parent-snap fork-snap) "branches actually diverged")
            (is (p/ancestor? fsys ca fork-snap)   "merge-base is an ancestor of the fork")
            (is (p/ancestor? fsys ca parent-snap) "merge-base is an ancestor of the parent")
            ;; the fork point is the base branch's head at branch time (base-snap),
            ;; or an ancestor of it — never a post-fork commit.
            (is (or (= ca base-snap) (p/ancestor? fsys ca base-snap))
                "merge-base is the fork point (or earlier), not a post-fork commit")))))))

(deftest conflicts-survives-gc-of-merge-base
  (testing "when retention GC reclaims the merge-base, conflict detection must NOT
            silently return [] — it falls back to a conservative 2-way check so a
            divergent stale fork still surfaces (never blind-merges)."
    ;; FILE backend: gc-storage needs a flushed index (memory leaves it unflushed).
    (let [dir (str (System/getProperty "java.io.tmpdir") "/ygg-gc-base-" (random-uuid))
          cfg {:store {:backend :file :path dir :id (random-uuid)}
               :keep-history? true :schema-flexibility :read}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (try
          (let [sys (dha/create conn {:system-name "t"})]
            (d/transact conn [{:db/ident :note/id   :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                              {:db/ident :note/text :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one}])
            ;; pad history so the fork point is an OLD intermediate commit
            (dotimes [i 30] (d/transact conn [{:note/id "n" :note/text (str "v" i)}]))
            (p/branch! sys :ours)
            (p/branch! sys :theirs)
            (let [osys (p/checkout sys :ours)
                  tsys (p/checkout sys :theirs)]
              (d/transact (:conn osys) [{:note/id "n" :note/text "ours-val"}])
              (d/transact (:conn tsys) [{:note/id "n" :note/text "theirs-val"}])
              ;; base present → precise 3-way = exactly the n clash
              (is (= 1 (count (p/conflicts osys (p/snapshot-id osys) (p/snapshot-id tsys))))
                  "3-way with base present finds exactly the n clash")
              ;; reclaim everything before now (collapses old snapshots incl. fork point)
              @(d/gc-storage conn (java.util.Date.))
              (let [base  (p/common-ancestor osys (p/snapshot-id osys) (p/snapshot-id tsys))
                    confs (p/conflicts osys (p/snapshot-id osys) (p/snapshot-id tsys))]
                (println :BASE-AFTER-GC base :N-CONFS (count confs) :CONFS confs)
                ;; THE INVARIANT (the whole point): the divergence is ALWAYS surfaced —
                ;; never a silent [] that would let the merge gate blind-merge.
                (is (seq confs) "divergence must surface even if the merge-base was GC'd")
                (is (some #(= [:note/id "n"] (:entity %)) confs) "n's clash is flagged"))))
          (finally (d/release conn) (d/delete-database cfg)))))))

(deftest baseless-conflicts-over-flag-conservatively
  (testing "the baseless fallback (used when the merge-base is unavailable) flags
            EVERY card-one attr that differs between the heads — including a
            one-sided change it can't prove is safe — so the merge gate escalates
            rather than blind-merges. Deterministically exercises the path the GC
            test can't (gc-storage keeps live branches' merge-base)."
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :note/id   :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :note/text :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
      (d/transact *conn* [{:note/id "n" :note/text "base"} {:note/id "m" :note/text "m-base"}])
      (p/branch! sys :ours)
      (p/branch! sys :theirs)
      (let [osys (p/checkout sys :ours)
            tsys (p/checkout sys :theirs)]
        ;; n: both change differently (true clash). m: only ours changes (one-sided).
        (d/transact (:conn osys) [{:note/id "n" :note/text "ours"} {:note/id "m" :note/text "m-ours"}])
        (d/transact (:conn tsys) [{:note/id "n" :note/text "theirs"}])
        (let [baseless (deref #'dha/compute-conflicts-baseless)
              confs    (baseless @(:conn osys) @(:conn tsys))]
          ;; over-flags: BOTH n (real) AND m (one-sided, can't prove safe w/o base)
          (is (= #{[:note/id "n"] [:note/id "m"]} (set (map :entity confs)))
              "flags the real clash AND conservatively the one-sided change")
          (is (every? #(= :unavailable (:base %)) confs)
              "entries tagged :base :unavailable (distinguishable from 3-way)"))))))

(deftest overlay-isolate-and-merge-down
  (testing "datahike overlay = a native branch fork; mutate it in isolation;
            merge-down! 3-way-merges it back into the parent branch"
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :note/id   :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :note/text :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
      (d/transact *conn* [{:note/id "n1" :note/text "base"}])
      (let [ov     (p/overlay sys {})
            forked (ovl/overlay-system ov)
            fconn  (:conn forked)
            ids    (fn [conn] (set (map first (d/q '[:find ?id :where [?e :note/id ?id]] @conn))))]
        (d/transact fconn [{:note/id "n2" :note/text "fork-only"}])
        (is (= #{"n1"}      (ids (:conn sys))) "parent branch untouched while overlay is open")
        (is (= #{"n1" "n2"} (ids fconn))       "fork branch has the isolated write")
        (let [merged (p/merge-down! ov)]
          (is (= #{"n1" "n2"} (ids (:conn merged)))
              "merge-down! 3-way-merged the fork's n2 into the parent branch"))
        (is (true? (:overlayable (p/capabilities sys))) "datahike advertises :overlayable")))))

(deftest merge-selects-by-identity-not-entity-id
  (testing "a fork's datom must not be dropped because the TARGET happens to hold
            the byte-identical datom for an UNRELATED entity.

            Two branches diverging from one base allocate new entity-ids from the
            SAME counter, so `new entity 8` on the fork and `new entity 8` on
            trunk are different things. Selecting the merge set with
            `(not [$target ?e ?a ?v])` compares those ids across the two dbs, so a
            fork datom silently vanishes whenever the numbers coincide and the
            value is low-cardinality — `:block/order \"a0\"`, a ref to eid 7, an
            amount of 42. merge! still reports :ok.

            The addressing was already identity-based; only the SELECTION was not."
    (let [sys (dha/create *conn* {:system-name "t"})]
      (d/transact *conn* [{:db/ident :leg/id     :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :leg/parent :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :leg/order  :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :tx/id      :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
      (d/transact *conn* [{:tx/id "base"}])
      (p/branch! sys :fork)
      (let [fsys (p/checkout sys :fork)]
        ;; FORK: a parent row and two legs under it, both with the same order key
        (d/transact (:conn fsys) [{:tx/id "F"}])
        (d/transact (:conn fsys) [{:leg/id "F-1" :leg/parent [:tx/id "F"] :leg/order "a0"}
                                  {:leg/id "F-2" :leg/parent [:tx/id "F"] :leg/order "a0"}])
        ;; TRUNK advances concurrently, allocating eids from the same counter and
        ;; writing the same low-cardinality values
        (d/transact *conn* [{:tx/id "T"}])
        (d/transact *conn* [{:leg/id "T-1" :leg/parent [:tx/id "T"] :leg/order "a0"}
                            {:leg/id "T-2" :leg/parent [:tx/id "T"] :leg/order "a0"}])
        (p/merge! sys :fork)
        (let [db  @*conn*
              legs (fn [tx] (set (d/q '[:find [?lid ...] :in $ ?tx :where
                                        [?t :tx/id ?tx] [?l :leg/parent ?t] [?l :leg/id ?lid]]
                                      db tx)))]
          (is (= #{"F" "T" "base"} (set (d/q '[:find [?id ...] :where [_ :tx/id ?id]] db)))
              "both parents present")
          (is (= #{"F-1" "F-2"} (legs "F"))
              "BOTH fork legs kept their parent link — neither dropped by an eid collision")
          (is (= #{"T-1" "T-2"} (legs "T"))
              "trunk's own legs untouched")
          (is (empty? (d/q '[:find [?lid ...] :where
                             [?l :leg/id ?lid] (not [?l :leg/parent _])] db))
              "no orphan legs — a dropped :leg/parent leaves the row unreachable
               from its transaction, invisible to any join-based report"))))))
