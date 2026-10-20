(ns yggdrasil.historical-overlay-test
  (:require [clojure.test :refer [deftest is]]
            [yggdrasil.protocols :as p]))

(defn- system [advertised? calls result]
  (reify
    p/SystemIdentity
    (system-id [_] "historical-contract-fixture")
    (system-type [_] :fixture)
    (capabilities [_] {:snapshotable true :overlayable true
                       :branchable false :snapshot-overlay advertised?})
    p/Snapshotable
    (snapshot-id [_] "current")
    (parent-ids [_] #{})
    (as-of [_ _] nil)
    (as-of [_ _ _] nil)
    (snapshot-meta [_ _] nil)
    (snapshot-meta [_ _ _] nil)
    ;; Protocol satisfaction is deliberately NOT a capability: this mimics a
    ;; no-op branch facade, without permitting it to authorize historical work.
    p/Branchable
    (branches [_] #{})
    (branches [_ _] #{})
    (current-branch [_] :main)
    (branch! [this _] this)
    (branch! [this _ _] this)
    (branch! [this _ _ _] this)
    (delete-branch! [this _] this)
    (delete-branch! [this _ _] this)
    (checkout [this _] this)
    (checkout [this _ _] this)
    p/Overlayable
    (overlay [_ opts] (swap! calls conj opts) result)
    (advance! [this] this)
    (advance! [this _] this)
    (peek-parent [_] nil)
    (peek-parent [_ _] nil)
    (base-ref [_] nil)
    (overlay-writes [_] nil)
    (merge-down! [_] nil)
    (merge-down! [_ _] nil)
    (discard! [_] nil)
    (discard! [_ _] nil)))

(deftest historical-capability-is-positive-not-protocol-satisfaction
  (let [calls (atom [])]
    (doseq [advertised [nil false :yes 1]]
      (let [s (system advertised calls :must-not-be-returned)]
        (is (satisfies? p/Branchable s))
        (is (thrown? clojure.lang.ExceptionInfo (p/overlay-at s "past")))))
    (is (empty? @calls))
    (is (thrown? clojure.lang.ExceptionInfo
                 (p/overlay-at {:snapshot-overlay true} "past")))))

(deftest malformed-or-following-history-refuses-before-adapter-work
  (let [calls (atom []) s (system true calls :overlay)]
    (doseq [[snapshot opts] [[nil nil] ["" nil] [:main nil]
                             ["past" []] ["past" {:mode :following}]
                             ["past" {:mode nil}] ["past" {:mode :gated}]
                             ["past" {:snapshot "another"}]]]
      (is (thrown? clojure.lang.ExceptionInfo (p/overlay-at s snapshot opts))))
    (is (empty? @calls))))

(deftest delegates-the-exact-request-and-preserves-value-or-cps-duality
  (let [calls (atom [])
        operation (fn [_resolve _reject] (throw (AssertionError. "Must not eagerly execute CPS")))
        snapshot (random-uuid)]
    (is (= :overlay (p/overlay-at (system true calls :overlay) "past")))
    (is (= [{:snapshot "past" :mode :frozen}] @calls))
    (is (identical? operation
                    (p/overlay-at (system true calls operation) snapshot
                                  {:sync? false :mode :frozen})))
    (is (= {:snapshot snapshot :mode :frozen :sync? false} (last @calls)))))
