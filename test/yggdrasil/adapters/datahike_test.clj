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
