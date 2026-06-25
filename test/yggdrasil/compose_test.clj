(ns yggdrasil.compose-test
  "`yggdrasil.compose` — mechanical multi-system overlay lifecycle over a LOOSE bag
   of independent Overlayable systems (distinct from `composite`, one wrapped system,
   and `workspace`, HLC+registry). The orchestration logic (order, stop-on-failure,
   rollback-of-remainder) is tested with reified Overlayable mocks; `prepare-all` /
   `snapshot-refs` are tested against real durable G-Sets."
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.compose :as compose]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent.gset :as g]
            [yggdrasil.convergent.overlay :as ovl]))

(defn- mem-gset [id]
  (g/gset id {:store-config {:backend :memory :id (random-uuid)}}))

;; A controllable Overlayable: records each merge-down!/discard! into `log`; `fail?`
;; makes merge-down! throw (to exercise the rollback path). Only the 1-arity methods
;; `commit-seq!`/`discard-all!` actually call are implemented.
(defn- mock-overlay [id log fail?]
  (reify p/Overlayable
    (merge-down! [_]
      (if fail?
        (throw (ex-info "boom" {:id id}))
        (do (swap! log conj [:merge id]) id)))
    (discard! [_]
      (swap! log conj [:discard id])
      nil)))

(deftest prepare-all-overlays-each-system
  (let [a (-> (mem-gset "a") (g/conj :a))
        b (mem-gset "b")
        overlays (compose/prepare-all [a b] {:mode :frozen})]
    (is (= #{"a" "b"} (set (keys overlays))) "keyed by system-id")
    (is (every? ovl/overlay? (vals overlays)) "each value is an Overlay")))

(deftest commit-seq-merges-down-in-order
  (testing "every overlay merges down, in order, all committed"
    (let [log (atom [])
          ovs [(mock-overlay :a log false)
               (mock-overlay :b log false)
               (mock-overlay :c log false)]
          r (compose/commit-seq! ovs)]
      (is (= 3 (count (:committed r))))
      (is (nil? (:failed r)))
      (is (empty? (:discarded r)))
      (is (nil? (:error r)))
      (is (= [[:merge :a] [:merge :b] [:merge :c]] @log) "merged in order"))))

(deftest commit-seq-stops-and-discards-remainder-on-failure
  (testing "on first failure: prior commits kept, failing one reported, rest discarded"
    (let [log (atom [])
          a   (mock-overlay :a log false)
          bad (mock-overlay :bad log true)
          c   (mock-overlay :c log false)
          r   (compose/commit-seq! [a bad c])]
      (is (= [a] (:committed r)) "a committed before the failure")
      (is (= bad (:failed r)) "bad is the failing overlay")
      (is (= [c] (:discarded r)) "c (remaining) abandoned")
      (is (some? (:error r)) "the exception is surfaced")
      (is (= [[:merge :a] [:discard :c]] @log)
          "a merged, bad threw (no merge logged), c discarded — no merge of c"))))

(deftest discard-all-discards-each
  (let [log (atom [])
        ovs [(mock-overlay :a log false) (mock-overlay :b log false)]]
    (compose/discard-all! ovs)
    (is (= [[:discard :a] [:discard :b]] @log))))

(deftest snapshot-refs-collects-per-system
  (let [a (-> (mem-gset "a") (g/conj :a))
        b (mem-gset "b")
        refs (compose/snapshot-refs [a b])]
    (is (= #{"a" "b"} (set (keys refs))))
    (is (= :gset (:system-type (get refs "a"))))
    (is (string? (:snapshot-id (get refs "a"))) "a content-addressed snapshot id")))
