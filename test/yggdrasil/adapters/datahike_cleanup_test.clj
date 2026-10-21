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
                                                (when (= :branch-removal fault) (throw failure)))]
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

(deftest concurrent-discard-cannot-decrement-the-same-lease-twice
  (let [{:keys [owner child] :as f} (fixture)
        entered (promise) proceed (promise) calls (atom [])]
    (open! f)
    (with-redefs [d/release (fn [conn]
                              (swap! calls conj [:release conn])
                              (when (identical? child conn) (deliver entered true) @proceed))
                  versioning/delete-branch! (fn [& _] (swap! calls conj :remove))]
      (let [first (future (cleanup/discard! owner {}))]
        (try
          (is (= true (deref entered 5000 :timeout)))
          (is (thrown? clojure.lang.ExceptionInfo (cleanup/discard! owner {})))
          (is (= 1 (count @calls)))
          (finally (deliver proceed true)))
        (is (nil? (deref first 5000 :timeout)))
        (is (nil? (cleanup/discard! owner {})))
        (is (= 3 (count @calls)))))))

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
