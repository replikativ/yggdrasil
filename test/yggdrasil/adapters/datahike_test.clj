(ns yggdrasil.adapters.datahike-test
  "Graphable regression tests for the Datahike adapter.

   These methods were previously untested, which let a type bug survive:
   walk-history collected commit-ids as UUID objects while every consumer
   (ancestor?, common-ancestor, commit-graph) compared with (str …), so a UUID
   never matched its own string in a set lookup and common-ancestor always
   returned nil — silently breaking fork merge-base derivation."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [yggdrasil.adapters.datahike :as dha]
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
