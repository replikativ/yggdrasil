(ns yggdrasil.adapters.datahike-cleanup
  "Native ownership of overlay cleanup, independent of temporary parents.
   Optional Datahike dependency. Owners are opaque process-local resources,
   not portable semantic state, transport receipts or authorization."
  (:require [datahike.api :as d]
            [datahike.versioning :as versioning]
            [datahike.store :as store]
            [is.simm.partial-cps.core-async :as ca]
            #?(:clj [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(deftype CleanupOwner [state]
  Object
  (toString [_] "<native-overlay-cleanup-owner>"))

(defn- state [owner] (.-state ^CleanupOwner owner))
(defn- sync? [opts] (get opts :sync? #?(:clj true :cljs false)))

(defn info
  "Safe diagnostic data only; no native connections or configuration."
  [owner]
  (select-keys @(state owner) [:branch :snapshot :stage :cleanup-phase :in-flight :uncertain]))

(defn- uncertain! [owner message cause]
  (let [details (info owner)]
    (ex-info message (merge (select-keys details [:branch :snapshot])
                            {:type (case (:in-flight details)
                                     :branch-allocation :yggdrasil/overlay-allocation-uncertain
                                     (:canonical-connect :child-connect) :yggdrasil/overlay-connection-uncertain
                                     :yggdrasil/overlay-cleanup-uncertain)
                             :cleanup details ::owner owner}) cause)))

(defn owner
  "Capture source configuration while it is valid, without acquiring anything."
  [conn branch snapshot]
  (let [cfg (:config @conn)]
    (when-not (uuid? (store/store-identity (:store cfg)))
      (throw (ex-info "Overlay cleanup requires an identified native source store" {})))
    (when (= :db branch)
      (throw (ex-info "Overlay cleanup cannot own the canonical branch" {})))
    (CleanupOwner.
     (atom {:config (assoc cfg :branch :db) :store (store/store-identity (:store cfg))
            :branch branch :snapshot snapshot :stage :new
            :cleanup-phase :not-started :in-flight nil :uncertain nil}))))

(defn- begin! [owner operation]
  (let [a (state owner)]
    (loop []
      (let [before @a]
        (cond
          (and (= :cleanup operation) (= :closed (:cleanup-phase before))) false
          (or (:uncertain before) (:in-flight before))
          (throw (uncertain! owner "Overlay operation is pending or uncertain; do not retry automatically" nil))
          (not= (:stage before) (case operation
                                  :canonical-connect :new :branch-allocation :canonical-open
                                  :child-connect :branch-open :cleanup :child-open))
          (throw (uncertain! owner "Overlay operation has an invalid ownership phase" nil))
          (compare-and-set! a before (assoc before :in-flight operation)) true
          :else (recur))))))

(defn- acknowledged! [owner values]
  (swap! (state owner)
         #(cond-> (merge % values {:in-flight nil})
            (= :closed (:cleanup-phase values))
            (dissoc :config :store :child-conn :cleanup-conn))))

(defn- failed! [owner error]
  (swap! (state owner) #(assoc % :uncertain (:in-flight %)))
  (throw (uncertain! owner "Overlay native operation did not acknowledge completion" error)))

(defn acquire!
  "Acquire exactly one canonical cleanup lease before allocating any branch."
  [owner opts]
  (let [s? (sync? opts)]
    (async+sync s?
                (async
                 (begin! owner :canonical-connect)
                 (try
                   (let [conn (await (ca/sync-or-cps (d/connect (:config @(state owner)) {:sync? s?}) {:sync? s?}))]
           ;; Record the ACK even when identity validation refuses; the owner
           ;; must not lose a returned lease on that error path.
                     (swap! (state owner) assoc :cleanup-conn conn)
                     (when-not (and (= :db (get-in @conn [:config :branch]))
                                    (= (:store @(state owner)) (store/store-identity (get-in @conn [:config :store]))))
                       (throw (ex-info "Canonical cleanup lease has a different store identity" {})))
                     (acknowledged! owner {:stage :canonical-open})
                     owner)
                   (catch #?(:clj Throwable :cljs :default) error (failed! owner error)))))))

(defn allocating! [owner] (begin! owner :branch-allocation))
(defn allocated! [owner] (acknowledged! owner {:stage :branch-open}))
(defn connect-child!
  "Connect the private branch using configuration captured before allocation,
   not a borrowed parent that may since have settled."
  [owner opts]
  (let [s? (sync? opts)]
    (async+sync s?
                (async
                 (begin! owner :child-connect)
                 (try
                   (let [{:keys [config branch]} @(state owner)
                         conn (await (ca/sync-or-cps (d/connect (assoc config :branch branch) {:sync? s?}) {:sync? s?}))]
                     (swap! (state owner) assoc :child-conn conn)
                     (when-not (and (= branch (get-in @conn [:config :branch]))
                                    (= (:store @(state owner)) (store/store-identity (get-in @conn [:config :store]))))
                       (throw (ex-info "Overlay child connection has a different native identity" {})))
                     (acknowledged! owner {:stage :child-open :child-conn conn})
                     conn)
                   (catch #?(:clj Throwable :cljs :default) error (failed! owner error)))))))
(defn construction-failed!
  "Retain an uncertain construction owner, including acquisition locators.
   Does not infer that a throwing allocator/connect had no side effects."
  [owner error]
  (if (identical? owner (::owner (ex-data error)))
    (throw error)
    (failed! owner error)))

(defn discard!
  "Release child, remove branch through our canonical lease, then release that
   lease. Closed owners are idempotent. In-flight/uncertain outcomes refuse
   replay and retain the owner; a later explicit reconciliation is separate."
  [owner opts]
  (let [s? (sync? opts)]
    (async+sync s?
                (async
                 (when (begin! owner :cleanup)
                   (try
                     (let [{:keys [child-conn cleanup-conn branch stage]} @(state owner)]
                       (when-not (= :child-open stage)
                         (throw (ex-info "Overlay construction has no complete child ownership acknowledgement" {})))
               ;; Keep an in-flight marker around each operation. No later
               ;; call repeats a successful release after an intervening fault.
                       (swap! (state owner) assoc :in-flight :child-release)
                       (d/release child-conn)
                       (swap! (state owner) assoc :cleanup-phase :child-released :in-flight :branch-removal)
                       (await (ca/sync-or-cps (versioning/delete-branch! cleanup-conn branch {:sync? s?}) {:sync? s?}))
                       (swap! (state owner) assoc :cleanup-phase :branch-removed :in-flight :canonical-release)
                       (d/release cleanup-conn)
                       (acknowledged! owner {:cleanup-phase :closed})
                       nil)
                     (catch #?(:clj Throwable :cljs :default) error (failed! owner error))))))))
