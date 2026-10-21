(ns yggdrasil.adapters.cleanup-test-support
  "TEST ONLY diagnostic cleanup for injections that prove a native operation
   did not run, or independently retain its exact lost-ACK connection. This
   knowledge is not inferred from an arbitrary production exception."
  (:require [datahike.api :as d]
            [yggdrasil.adapters.datahike-cleanup :as cleanup]))

(defn owner-of [error]
  (:yggdrasil.adapters.datahike-cleanup/owner (ex-data error)))

(defn state-of [owner] @(@#'cleanup/state owner))

(defn dispose-known! [owner & [observed-lost-child]]
  (let [{:keys [child-conn cleanup-conn branch cleanup-phase]} (state-of owner)]
    (when-not (= :closed cleanup-phase)
      (when (and child-conn (not (#{:child-released :branch-removed} cleanup-phase)))
        (d/release child-conn))
      (when observed-lost-child (d/release observed-lost-child))
      (when (contains? (d/branches cleanup-conn) branch)
        (d/delete-branch! cleanup-conn branch))
      (d/release cleanup-conn))))
