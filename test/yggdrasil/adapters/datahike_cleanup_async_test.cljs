(ns yggdrasil.adapters.datahike-cleanup-async-test
  "Exercise the portable owner's actual async channel/CPS path in Node. These
   native mocks are scoped to one asynchronous test, never application state."
  (:require [cljs.test :refer-macros [is]]
            [cljs.core.async :as a]
            [datahike.api :as d]
            [datahike.versioning :as versioning]
            [is.simm.partial-cps.async]
            [is.simm.partial-cps.core-async]
            [yggdrasil.adapters.datahike-cleanup :as cleanup]
            [yggdrasil.test-async :refer-macros [deftest-async <?]]))

(defn- channel [value]
  (let [ch (a/promise-chan)]
    ;; Dispatch delivery separately so both success and failure cross the
    ;; channel/CPS suspension, rather than using an already-buffered datum.
    (a/go (a/>! ch value))
    ch))

(deftest-async async-channel-acknowledgements-and-uncertainty-preserve-ownership
  (let [cfg {:store {:backend :memory :id (random-uuid)} :branch :db}
        root (atom {:config cfg})
        child (atom {:config (assoc cfg :branch :async-child)})
        original-connect d/connect
        original-release d/release
        original-delete versioning/delete-branch!
        calls (atom [])
        fail-delete? (atom false)]
    ;; Keep the mocks installed THROUGH async completion. Restoring them when a
    ;; continuation is merely returned would test the wrong implementations.
    (set! d/connect (fn [requested opts]
                      (is (false? (:sync? opts)))
                      (swap! calls conj [:connect (:branch requested)])
                      (channel (if (= :db (:branch requested)) root child))))
    (set! d/release (fn [conn]
                      (swap! calls conj [:release (get-in @conn [:config :branch])])
                      nil))
    (set! versioning/delete-branch!
          (fn
            ([_conn _branch]
             (throw (js/Error. "Expected explicit async deletion options")))
            ([conn branch opts]
             (is (false? (:sync? opts)))
             (swap! calls conj [:remove (get-in @conn [:config :branch]) branch])
             (channel (if @fail-delete? (js/Error. "Injected missing deletion ACK") true)))))
    (try
      (let [owner (cleanup/owner root :async-child "basis")]
        (<? (cleanup/acquire! owner {:sync? false}))
        (cleanup/allocating! owner)
        (cleanup/allocated! owner)
        (is (identical? child (<? (cleanup/connect-child! owner {:sync? false}))))
        (<? (cleanup/discard! owner {:sync? false}))
        (<? (cleanup/discard! owner {:sync? false}))
        (is (= :closed (:cleanup-phase (cleanup/info owner))))
        (is (= [[:connect :db] [:connect :async-child] [:release :async-child]
                [:remove :db :async-child] [:release :db]] @calls)))
      (reset! calls [])
      (reset! fail-delete? true)
      (let [owner (cleanup/owner root :async-child "basis")]
        (<? (cleanup/acquire! owner {:sync? false}))
        (cleanup/allocating! owner)
        (cleanup/allocated! owner)
        (<? (cleanup/connect-child! owner {:sync? false}))
        (let [failure (try (<? (cleanup/discard! owner {:sync? false})) nil
                           (catch :default error error))
              before @calls
              replay (try (<? (cleanup/discard! owner {:sync? false})) nil
                          (catch :default error error))]
          (is (some? failure))
          (is (some? replay))
          (is (= :branch-removal (:uncertain (cleanup/info owner))))
          (is (= :child-released (:cleanup-phase (cleanup/info owner))))
          (is (= before @calls))
          (is (not-any? #(= [:release :db] %) @calls))))
      (catch :default error
        (is false (pr-str {:unexpected (select-keys (ex-data error) [:type :cleanup])
                           :cause (some-> error ex-cause ex-message)
                           :calls @calls})))
      (finally
        (set! d/connect original-connect)
        (set! d/release original-release)
        (set! versioning/delete-branch! original-delete)))))
