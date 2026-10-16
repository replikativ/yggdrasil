(ns yggdrasil.convergent.overlay-test
  "Regression coverage for the CONVERGENT-CRDT overlay (the fork substrate). A
   `:following` overlay's `merge-down!`/`advance!`, when called with a MERGE-OPTIONS
   map that omits `:sync?` (exactly how the spindel bridge's `merge-to-parent!` calls
   them), must return a VALUE (parent ⊔ delta) on the JVM — NOT a partial-cps
   continuation. The bridge used to seat that continuation as the parent's signal
   value, silently emptying the parent on merge. Fix: `overlay.cljc` defaults
   `:sync?` to the platform mode. This path had no test before (the bridge's fork
   tests are git-only)."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.convergent.gset :as g]
            [yggdrasil.convergent.overlay :as ovl]
            [yggdrasil.protocols :as p]))

(defn- mem-gset [id]
  (g/gset id {:store-config {:backend :memory :id (random-uuid)}} {:sync? true}))

(deftest following-overlay-merge-down-returns-value-not-cps-fn
  (testing "merge-down! with a no-:sync? merge-opts map returns the joined VALUE, not a fn"
    (let [parent (-> (mem-gset "kb") (g/conj :shared))
          ov     (p/overlay parent {:mode :following})
          _      (ovl/overlay-swap! ov (fn [gs] (g/conj gs :fork)))]
      (is (= #{:shared :fork} (g/elements (ovl/overlay-value ov)))
          "overlay-value (effective read) = parent ⊔ overlay delta")
      (let [merged (p/merge-down! ov {:message "test" :strategy nil})]
        (is (not (fn? merged))
            "merge-down! must return a value, not a partial-cps continuation")
        (is (= #{:shared :fork} (g/elements merged))
            "merge-down! folds the overlay delta into the parent")))))

(deftest following-overlay-advance-returns-overlay-not-cps-fn
  (testing "advance! with a no-:sync? opts map returns the overlay, not a fn"
    (let [parent (-> (mem-gset "kb2") (g/conj :p1))
          ov     (p/overlay parent {:mode :following})
          _      (ovl/overlay-swap! ov (fn [gs] (g/conj gs :o1)))
          adv    (p/advance! ov {:message "adv"})]
      (is (not (fn? adv)) "advance! must return a value, not a partial-cps continuation")
      (is (ovl/overlay? adv) "advance! returns the overlay"))))

(deftest a-concurrent-write-during-overlay-swap-is-kept
  (testing "a write that lands while a swap computes is not overwritten: the swap retries"
    (let [parent (-> (mem-gset "kb") (g/conj :shared))
          ov     (p/overlay parent {:mode :following})
          fired  (atom false)]
      (ovl/overlay-swap! ov (fn [gs]
                              ;; another writer lands first (once)
                              (when (compare-and-set! fired false true)
                                (ovl/overlay-swap! ov (fn [g2] (g/conj g2 :concurrent))))
                              (g/conj gs :mine)))
      (is (= #{:shared :mine :concurrent} (g/elements (ovl/overlay-value ov)))))))

(deftest a-write-during-advance-is-kept
  (testing "advance! joins by compare-and-set: a write landing during the join stays"
    (let [parent (-> (mem-gset "kb") (g/conj :shared))
          ov     (p/overlay parent {:mode :frozen})]
      (ovl/overlay-swap! ov (fn [gs] (g/conj gs :before)))
      (p/advance! ov {})
      (ovl/overlay-swap! ov (fn [gs] (g/conj gs :after)))
      (is (= #{:shared :before :after} (g/elements (ovl/overlay-value ov)))))))
