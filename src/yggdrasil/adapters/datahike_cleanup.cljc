(ns yggdrasil.adapters.datahike-cleanup
  "Native ownership of overlay cleanup, independent of temporary parents.
   Optional Datahike dependency. Owners are opaque process-local resources,
   not portable semantic state, transport receipts or authorization."
  (:require #?(:clj [clojure.core.async :as a]
               :cljs [cljs.core.async :as a])
            [datahike.api :as d]
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

(defn- begin!
  "Mark `operation` in flight. Cleanup instead answers nil once closed, or
   `[:join done]` while another cleanup runs, else `[:run done]`."
  [owner operation]
  (let [a (state owner)]
    (loop []
      (let [before @a]
        (cond
          (and (= :cleanup operation) (= :closed (:cleanup-phase before))) nil
          (and (= :cleanup operation) (:cleanup-run before) (not (:uncertain before)))
          [:join (:cleanup-run before)]
          (or (:uncertain before) (:in-flight before))
          (throw (uncertain! owner "Overlay operation is pending or uncertain; do not retry automatically" nil))
          (not (contains? (case operation
                            :canonical-connect #{:new} :branch-allocation #{:canonical-open}
                            :child-connect #{:branch-open} :cleanup #{:branch-open :child-open})
                          (:stage before)))
          (throw (uncertain! owner "Overlay operation has an invalid ownership phase" nil))
          :else
          (let [done (when (= :cleanup operation) (a/promise-chan))]
            (if (compare-and-set! a before (cond-> (assoc before :in-flight operation)
                                             done (assoc :cleanup-run done)))
              (if done [:run done] true)
              (recur))))))))

(defn- acknowledged! [owner values]
  (swap! (state owner)
         #(cond-> (merge % values {:in-flight nil})
            (= :closed (:cleanup-phase values))
            (dissoc :config :store :child-conn :cleanup-conn :cleanup-run))))

(defn- failed! [owner error]
  (swap! (state owner) #(-> % (assoc :uncertain (:in-flight %)) (dissoc :cleanup-run)))
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
(declare discard!)

(defn- registry [conn s?]
  (ca/sync-or-cps (versioning/branches conn {:sync? s?}) {:sync? s?}))

(defn- with-primary [cleanup-error primary]
  (ex-info (ex-message cleanup-error)
           (assoc (ex-data cleanup-error) :cleanup-errors [cleanup-error]) primary))

(defn construction-failed!
  "Resolve or retain a failed construction step. A throwing branch allocation
   is resolved by observing the branch registry through our canonical lease,
   never inferred from the exception: an unregistered branch was not allocated,
   so the lease is released; a registered one was, so it is disposed. Either way
   the native error is rethrown. Any other step, or a failed observation,
   retains an uncertain owner with its acquisition locators."
  ([owner error] (construction-failed! owner error {}))
  ([owner error opts]
   (let [s? (sync? opts)]
     (async+sync s?
                 (async
                  (cond
                    (identical? owner (::owner (ex-data error))) (throw error)
                    (not= :branch-allocation (:in-flight @(state owner))) (failed! owner error)
                    :else
                    (let [{:keys [cleanup-conn branch]} @(state owner)
                          registered (try
                                       (contains? (set (await (registry cleanup-conn s?))) branch)
                                       (catch #?(:clj Throwable :cljs :default) _ (failed! owner error)))]
                      (if registered
                        (do (acknowledged! owner {:stage :branch-open})
                            (try
                              (await (discard! owner opts))
                              (catch #?(:clj Throwable :cljs :default) cleanup-error
                                (throw (with-primary cleanup-error error)))))
                        (do (swap! (state owner) assoc :in-flight :canonical-release)
                            (try (d/release cleanup-conn)
                                 (catch #?(:clj Throwable :cljs :default) release-error
                                   (swap! (state owner) assoc :uncertain :canonical-release)
                                   (throw (with-primary (uncertain! owner "Overlay native operation did not acknowledge completion"
                                                                    release-error)
                                            error))))
                            (acknowledged! owner {:cleanup-phase :closed})))
                      (throw error))))))))

(defn- join
  "Await the outcome of the cleanup that is already running."
  [done s?]
  (if s?
    #?(:clj (ca/unwrap-result (a/<!! done))
       :cljs (throw (ex-info "Overlay cleanup is pending; join it asynchronously" {})))
    (ca/chan->cps done)))

(defn- remove-branch!
  "Remove the branch through our lease. A throwing removal is resolved by
   reading the registry, as for allocation: unregistered means removed; still
   registered means not removed, a definite failure that may be retried. A
   failed read rethrows the removal error, which leaves the owner uncertain."
  [owner s?]
  (async+sync s?
              (async
               (let [{:keys [cleanup-conn branch]} @(state owner)]
                 (try
                   (await (ca/sync-or-cps (versioning/delete-branch! cleanup-conn branch {:sync? s?}) {:sync? s?}))
                   (catch #?(:clj Throwable :cljs :default) error
                     (when (try (contains? (set (await (registry cleanup-conn s?))) branch)
                                (catch #?(:clj Throwable :cljs :default) _ (throw error)))
                       (throw (ex-info "Overlay branch removal failed; discard! may be retried"
                                       (merge (select-keys (info owner) [:branch :snapshot])
                                              {:type :yggdrasil/overlay-cleanup-failed
                                               :cleanup (info owner) ::owner owner})
                                       error)))))))))

(defn discard!
  "Release child, remove branch through our canonical lease, then release that
   lease. Closed owners are idempotent, and a concurrent call joins the running
   cleanup's outcome. A failed branch removal that left the branch registered
   is definite: the owner stays retryable and a later call resumes there.
   Uncertain outcomes refuse replay and retain the owner."
  [owner opts]
  (let [s? (sync? opts)]
    (async+sync s?
                (async
                 (when-let [[role done] (begin! owner :cleanup)]
                   (if (= :join role)
                     (do (await (join done s?)) nil)
                     (try
                       (let [{:keys [child-conn cleanup-conn stage cleanup-phase]} @(state owner)]
               ;; Keep an in-flight marker around each operation. No later
               ;; call repeats a successful release after an intervening fault.
               ;; An allocated branch without a child connection has none to
               ;; release; a retry resumes after the acknowledged release.
                         (when (and (= :child-open stage) (= :not-started cleanup-phase))
                           (swap! (state owner) assoc :in-flight :child-release)
                           (d/release child-conn))
                         (swap! (state owner) assoc :cleanup-phase :child-released :in-flight :branch-removal)
                         (await (remove-branch! owner s?))
                         (swap! (state owner) assoc :cleanup-phase :branch-removed :in-flight :canonical-release)
                         (d/release cleanup-conn)
                         (acknowledged! owner {:cleanup-phase :closed})
                         (a/put! done ca/sentinel-nil)
                         nil)
                       (catch #?(:clj Throwable :cljs :default) error
                         (let [outcome (if (and (= :yggdrasil/overlay-cleanup-failed (:type (ex-data error)))
                                                (identical? owner (::owner (ex-data error))))
                                         (do (swap! (state owner) #(-> % (assoc :in-flight nil) (dissoc :cleanup-run)))
                                             error)
                                         (try (failed! owner error)
                                              (catch #?(:clj Throwable :cljs :default) uncertain uncertain)))]
                           (a/put! done outcome)
                           (throw outcome))))))))))
