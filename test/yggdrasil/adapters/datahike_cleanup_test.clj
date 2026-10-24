(ns yggdrasil.adapters.datahike-cleanup-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.versioning :as versioning]
            [yggdrasil.adapters.datahike-cleanup :as cleanup]))

(defn- fixture []
  (let [cfg {:store {:backend :memory :id (random-uuid)} :branch :db}
        source (atom cfg)
        root (atom {:config cfg})
        child (atom {:config (assoc cfg :branch :cleanup-test)})]
    {:owner (cleanup/owner (atom {:config cfg}) :cleanup-test "snapshot")
     :root root :child child :source source}))

(defn- open! [{:keys [owner root child]}]
  (with-redefs [d/connect (fn [cfg _] (if (= :db (:branch cfg)) root child))]
    (cleanup/acquire! owner {})
    (cleanup/allocating! owner)
    (cleanup/allocated! owner)
    (cleanup/connect-child! owner {}))
  owner)

(deftest cleanup-order-and-idempotence-do-not-depend-on-the-parent
  (let [{:keys [owner root child] :as f} (fixture)
        calls (atom [])]
    (open! f)
    (with-redefs [d/release #(swap! calls conj [:release %])
                  versioning/delete-branch! (fn [conn branch _] (swap! calls conj [:remove conn branch]))]
      (is (nil? (cleanup/discard! owner {})))
      (is (nil? (cleanup/discard! owner {})))
      (is (= [[:release child] [:remove root :cleanup-test] [:release root]] @calls))
      (is (= :closed (:cleanup-phase (cleanup/info owner))))
      (is (empty? (select-keys @(@#'cleanup/state owner) [:config :child-conn :cleanup-conn]))))))

(deftest cleanup-faults-retain-the-opaque-owner-and-refuse-replay
  (doseq [fault [:child-release :branch-removal :canonical-release]]
    (let [{:keys [owner root child] :as f} (fixture)
          calls (atom [])
          failure (ex-info "Injected missing ACK" {})]
      (open! f)
      (with-redefs [d/release (fn [conn]
                                (let [phase (if (identical? conn child) :child-release :canonical-release)]
                                  (swap! calls conj phase)
                                  (when (= phase fault) (throw failure))))
                    versioning/delete-branch! (fn [conn _ _]
                                                (is (identical? root conn))
                                                (swap! calls conj :branch-removal)
                                                (when (= :branch-removal fault) (throw failure)))
                    ;; A removal whose registry cannot be read stays uncertain.
                    versioning/branches (fn [& _] (throw (ex-info "Registry read failed" {})))]
        (let [error (try (cleanup/discard! owner {}) (catch Exception error error))
              before @calls]
          (is (identical? failure (ex-cause error)))
          (is (= fault (:uncertain (cleanup/info owner))))
          (is (= (case fault :child-release :not-started
                       :branch-removal :child-released :canonical-release :branch-removed)
                 (:cleanup-phase (cleanup/info owner))))
          (is (thrown? clojure.lang.ExceptionInfo (cleanup/discard! owner {})))
          (is (= before @calls))
          (is (not (contains? (ex-data error) :cleanup-conn)))
          (is (= "<native-overlay-cleanup-owner>" (str (:yggdrasil.adapters.datahike-cleanup/owner (ex-data error))))))))))

(deftest concurrent-discard-joins-the-running-cleanup
  (doseq [outcome [:closed :uncertain]]
    (let [{:keys [owner child] :as f} (fixture)
          entered (promise) proceed (promise) calls (atom [])
          failure (ex-info "Injected missing release ACK" {})]
      (open! f)
      (with-redefs [d/release (fn [conn]
                                (swap! calls conj [:release conn])
                                (when (identical? child conn)
                                  (deliver entered true)
                                  @proceed
                                  (when (= :uncertain outcome) (throw failure))))
                    versioning/delete-branch! (fn [& _] (swap! calls conj :remove))]
        (let [first (future (try (cleanup/discard! owner {}) (catch Exception error error)))
              _ (is (= true (deref entered 5000 :timeout)))
              second (future (try (cleanup/discard! owner {}) (catch Exception error error)))]
          (try
            (is (= :timeout (deref second 200 :timeout)) "The joiner waits for the outcome")
            (is (= 1 (count @calls)))
            (finally (deliver proceed true)))
          (case outcome
            :closed (do (is (nil? (deref first 5000 :timeout)))
                        (is (nil? (deref second 5000 :timeout)))
                        (is (= 3 (count @calls))))
            :uncertain (let [error (deref first 5000 :timeout)
                             joined (deref second 5000 :timeout)]
                         (is (identical? failure (ex-cause error)))
                         (is (identical? error (ex-cause joined)) "The joiner receives the same outcome")
                         (is (= :yggdrasil/overlay-cleanup-uncertain (:type (ex-data joined))))
                         (is (= 1 (count @calls)))))
          (is (= outcome (if (= :closed (:cleanup-phase (cleanup/info owner))) :closed :uncertain))))))))

(deftest definite-branch-removal-failure-is-retryable
  (doseq [registered? [true false]]
    (let [{:keys [owner root child] :as f} (fixture)
          calls (atom [])
          failure (ex-info "Injected konserve write error" {})
          fail? (atom true)]
      (open! f)
      (with-redefs [d/release #(swap! calls conj [:release %])
                    versioning/delete-branch! (fn [_ branch _]
                                                (swap! calls conj [:remove branch])
                                                (when @fail? (throw failure)))
                    versioning/branches (fn [conn _]
                                          (is (identical? root conn))
                                          (if registered? #{:db :cleanup-test} #{:db}))]
        (if registered?
          (let [error (try (cleanup/discard! owner {}) nil (catch Exception error error))]
            (is (identical? failure (ex-cause error)))
            (is (= :yggdrasil/overlay-cleanup-failed (:type (ex-data error))))
            (is (= {:cleanup-phase :child-released :in-flight nil :uncertain nil}
                   (select-keys (cleanup/info owner) [:cleanup-phase :in-flight :uncertain])))
            (reset! fail? false)
            (is (nil? (cleanup/discard! owner {})))
            (is (= [[:release child] [:remove :cleanup-test] [:remove :cleanup-test] [:release root]] @calls)
                "The retry resumes at removal; the child is released once"))
          (do (is (nil? (cleanup/discard! owner {})) "An unregistered branch was removed")
              (is (= [[:release child] [:remove :cleanup-test] [:release root]] @calls))))
        (is (= :closed (:cleanup-phase (cleanup/info owner))))))))

(deftest acquisition-faults-retain-locators-without-speculative-compensation
  (doseq [phase [:canonical-connect :child-connect]]
    (let [{:keys [owner root]} (fixture)
          calls (atom 0)]
      (with-redefs [d/connect (fn [_ _]
                                (swap! calls inc)
                                (if (or (= :canonical-connect phase) (> @calls 1))
                                  (throw (ex-info "Lost connect ACK" {})) root))
                    d/release (fn [& _] (throw (AssertionError. "Uncertain construction released a lease")))
                    versioning/delete-branch! (fn [& _] (throw (AssertionError. "Uncertain construction deleted a branch")))]
        (if (= :canonical-connect phase)
          (is (thrown? clojure.lang.ExceptionInfo (cleanup/acquire! owner {})))
          (do (cleanup/acquire! owner {})
              (cleanup/allocating! owner)
              (cleanup/allocated! owner)
              (is (thrown? clojure.lang.ExceptionInfo (cleanup/connect-child! owner {})))))
        (is (= phase (:uncertain (cleanup/info owner))))
        (let [before @calls]
          (is (thrown? clojure.lang.ExceptionInfo (cleanup/discard! owner {})))
          (is (= before @calls)))))))

(deftest acknowledged-wrong-identity-is-retained-before-validation-refuses
  (doseq [phase [:canonical-connect :child-connect]
          mismatch [:store :branch]]
    (let [{:keys [owner root]} (fixture)
          calls (atom 0)
          cfg (:config @root)
          wrong (atom {:config (case mismatch
                                 :store (assoc-in (assoc cfg :branch (if (= phase :canonical-connect) :db :cleanup-test))
                                                  [:store :id] (random-uuid))
                                 :branch (assoc cfg :branch :wrong))})]
      (with-redefs [d/connect (fn [& _]
                                (swap! calls inc)
                                (if (and (= phase :child-connect) (= 1 @calls)) root wrong))]
        (when (= phase :child-connect)
          (cleanup/acquire! owner {})
          (cleanup/allocating! owner)
          (cleanup/allocated! owner))
        (is (thrown? clojure.lang.ExceptionInfo
                     (if (= phase :canonical-connect) (cleanup/acquire! owner {})
                         (cleanup/connect-child! owner {}))))
        (is (= phase (:uncertain (cleanup/info owner))))
        (is (identical? wrong (get @(@#'cleanup/state owner)
                                   (if (= phase :canonical-connect) :cleanup-conn :child-conn))))
        (is (not= :child-open (:stage (cleanup/info owner))))))))

(deftest failed-allocation-is-resolved-through-the-owned-registry
  (doseq [observed [:unregistered :registered :unobservable :release-fails]]
    (let [{:keys [owner root]} (fixture)
          failure (ex-info "Injected allocation failure" {})
          calls (atom [])]
      (with-redefs [d/connect (fn [& _] root)
                    versioning/branches (fn [conn _]
                                          (is (identical? root conn))
                                          (case observed
                                            :unobservable (throw (ex-info "Registry read failed" {}))
                                            :registered #{:db :cleanup-test}
                                            #{:db}))
                    d/release (fn [conn]
                                (swap! calls conj :release)
                                (is (identical? root conn))
                                (when (= :release-fails observed) (throw (ex-info "Lost release ACK" {}))))
                    versioning/delete-branch! (fn [conn branch _]
                                                (is (identical? root conn))
                                                (swap! calls conj [:remove branch]))]
        (cleanup/acquire! owner {})
        (cleanup/allocating! owner)
        (let [error (try (cleanup/construction-failed! owner failure {}) nil
                         (catch Exception error error))]
          (case observed
            :unregistered (do (is (identical? failure error))
                              (is (= [:release] @calls))
                              (is (= :closed (:cleanup-phase (cleanup/info owner)))))
            :registered (do (is (identical? failure error))
                            (is (= [[:remove :cleanup-test] :release] @calls))
                            (is (= :closed (:cleanup-phase (cleanup/info owner)))))
            :unobservable (do (is (identical? failure (ex-cause error)))
                              (is (= :yggdrasil/overlay-allocation-uncertain (:type (ex-data error))))
                              (is (= :branch-allocation (:uncertain (cleanup/info owner))))
                              (is (empty? @calls)))
            :release-fails (do (is (identical? failure (ex-cause error)))
                               (is (= :yggdrasil/overlay-cleanup-uncertain (:type (ex-data error))))
                               (is (= :canonical-release (:uncertain (cleanup/info owner))))
                               (is (= [:release] @calls))))
          (when-not (= :closed (:cleanup-phase (cleanup/info owner)))
            (is (identical? owner (:yggdrasil.adapters.datahike-cleanup/owner (ex-data error))))
            (is (thrown? clojure.lang.ExceptionInfo (cleanup/discard! owner {})))
            (is (= (if (= :release-fails observed) [:release] []) @calls))))))))
