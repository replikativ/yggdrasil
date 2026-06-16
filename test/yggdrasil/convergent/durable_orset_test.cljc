(ns yggdrasil.convergent.durable-orset-test
  "Durable OR-Set + 2P-Set: add/observed-remove/re-add, add-wins under concurrency,
   cross-store convergence, durability, content-tag (registry shape), addressable
   freeze, overlay, δ-state. PORTABLE — one `.cljc` body per concern (JVM sync /
   cljs async via `<?`). VALUE SEMANTICS: mutators return new values; threaded."
  (:require [clojure.test :refer [is testing]]
            [yggdrasil.test-async :refer [deftest-async <? sync? mem]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-orset :as o]
            [yggdrasil.convergent.durable-2pset :as t]
            [yggdrasil.convergent.overlay :as ovl]
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [is.simm.partial-cps.async :refer [async]]))
  #?(:clj (:import [java.nio.file Files]
                   [java.nio.file.attribute FileAttribute])))

#?(:clj (defn- tmpdir [] (str (Files/createTempDirectory "ygg-orset" (make-array FileAttribute 0)))))

(defn- two-op-2pset
  "An overlay mutation that does TWO async ops (add :c, then remove :a). Two
   durable ops can't be `->`-chained on cljs (each returns a CPS), so the cljs
   branch sequences them in its own `async` block; the JVM branch is a plain
   sync thread. Returns the new 2P-Set (or an async resolving to it on cljs)."
  [c]
  #?(:clj  (-> (t/add c :c) (t/remove-elem :a))
     :cljs (async (<? (t/remove-elem (<? (t/add c :c)) :a)))))

(deftest-async add-remove-readd
  (testing "remove actually removes (unlike a G-Set); re-add brings it back"
    (let [s (<? (o/durable-orset "reg" :store-config (mem) :sync? sync?))
          s (<? (o/add s :x)) s (<? (o/add s :y))]
      (is (= #{:x :y} (<? (o/elements s))))
      (let [s (<? (o/remove-elem s :x))]
        (is (= #{:y} (<? (o/elements s))) "observed-remove drops :x")
        (let [s (<? (o/add s :x))]
          (is (= #{:x :y} (<? (o/elements s))) "re-add (fresh tag) brings :x back")
          (is (true? (c/-conflict-free? s)))
          (is (= :orset (p/system-type s))))))))

(deftest-async add-wins-concurrency
  (testing "a concurrent add the remover didn't observe SURVIVES the merge"
    (let [a (<? (o/durable-orset "reg" :store-config (mem) :sync? sync?))
          a (<? (o/add a :k))
          b (<? (o/durable-orset "reg" :store-config (mem) :sync? sync?))
          b (<? (o/add b :k))
          a (<? (o/remove-elem a :k))]
      (is (= #{} (<? (o/elements a))) "a removed its own :k")
      (let [a (<? (o/merge-peer! a b))]
        (is (= #{:k} (<? (o/elements a))) "add-wins: b's concurrent add survives a's remove")))))

(deftest-async cross-peer-converges
  (testing "two peers with disjoint add/remove ops converge to the same set"
    (let [a (<? (o/durable-orset "reg" :store-config (mem) :sync? sync?))
          a (<? (o/add a :a1)) a (<? (o/add a :shared))
          b (<? (o/durable-orset "reg" :store-config (mem) :sync? sync?))
          b (<? (o/add b :b1)) b (<? (o/add b :shared))
          b (<? (o/remove-elem b :shared))
          a (<? (o/flush! (<? (o/merge-peer! a b))))
          b (<? (o/flush! (<? (o/merge-peer! b a))))]
      (is (= (<? (o/elements a)) (<? (o/elements b))) "strong eventual consistency")
      (is (= #{:a1 :b1 :shared} (<? (o/elements a)))))))

(deftest-async content-tag-idempotent-add
  (testing "with a content-hash tag-fn, re-adding the same element is a no-op (registry shape)"
    (let [s (<? (o/durable-orset "reg" :store-config (mem) :sync? sync? :tag-fn identity))
          s (<? (o/add s :x)) s (<? (o/add s :x)) s (<? (o/add s :x))
          adds (<? (d/set->clj (:adds s) {:sync? sync?}))]
      (is (= #{:x} (<? (o/elements s))))
      (is (= 1 (count (filter (fn [[e _]] (= e :x)) adds)))
          "idempotent add: a single content-tagged pair"))))

(deftest-async orset-addressable-snapshot-freeze
  (testing "OR-Set snapshot-id (commit object) + as-of restores the frozen value"
    (let [s0  (<? (o/durable-orset "reg" :store-config (mem) :sync? sync?))
          s0  (<? (o/add s0 :a)) s0 (<? (o/add s0 :b))
          sid (<? (p/snapshot-id s0))
          s   (<? (o/remove-elem s0 :a)) s (<? (o/add s :c))]
      (is (= #{:b :c} (<? (o/elements s))))
      (is (= #{:a :b} (<? (p/as-of s sid))) "as-of restores the frozen live elements"))))

(deftest-async twopset-addressable-snapshot-freeze
  (testing "2P-Set snapshot-id (commit object) + as-of restores the frozen value"
    (let [s0  (<? (t/durable-2pset "x" :store-config (mem) :sync? sync?))
          s0  (<? (t/add s0 :a)) s0 (<? (t/add s0 :b))
          sid (<? (p/snapshot-id s0))
          s   (<? (t/remove-elem s0 :a))
          y0  (<? (t/durable-2pset "y" :store-config (mem) :sync? sync?))
          y0  (<? (t/add y0 :b)) y0 (<? (t/add y0 :a))
          ysid (<? (p/snapshot-id y0))]
      (is (= #{:b} (<? (t/elements s))))
      (is (= #{:a :b} (<? (p/as-of s sid))) "as-of restores the frozen value")
      (is (= sid ysid) "content-addressed commit: equal halves (any order/store) → equal snapshot-id"))))

(deftest-async orset-overlay-isolate-merge-down
  (testing "OR-Set overlay isolates (the residue fix); merge-down! joins (add-wins)"
    (let [s  (<? (o/durable-orset "reg" :store-config (mem) :sync? sync?))
          s  (<? (o/add s :a)) s (<? (o/add s :b))
          ov (p/overlay s {})]
      (<? (ovl/overlay-swap! ov (fn [c] (o/add c :c))))
      (is (= #{:a :b :c} (<? (o/elements (ovl/overlay-system ov)))) "overlay evolves in isolation")
      (is (= #{:a :b}    (<? (o/elements s)))                       "parent untouched while overlay is open")
      (is (= #{:a :b :c} (<? (o/elements (<? (p/merge-down! ov))))) "merge-down! joins the overlay"))))

(deftest-async twopset-overlay-isolate-merge-down
  (testing "2P-Set overlay isolates; merge-down! joins BOTH halves (remove propagates)"
    (let [s  (<? (t/durable-2pset "x" :store-config (mem) :sync? sync?))
          s  (<? (t/add s :a)) s (<? (t/add s :b))
          ov (p/overlay s {})]
      (<? (ovl/overlay-swap! ov two-op-2pset))
      (is (= #{:b :c} (<? (t/elements (ovl/overlay-system ov)))) "overlay evolves in isolation")
      (is (= #{:a :b} (<? (t/elements s)))                       "parent untouched while overlay is open")
      (is (= #{:b :c} (<? (t/elements (<? (p/merge-down! ov)))))
          "merge-down! unions adds+removals — the overlay's remove of :a propagates"))))

(deftest-async orset-store-layout-is-crdt-walkable
  (testing "both halves live under :crdt/roots — the shape konserve-sync's crdt walker syncs"
    (let [s (<? (o/durable-orset "reg" :store-config (mem) :sync? sync?))
          s (<? (o/add s :a)) s (<? (o/add s :b)) s (<? (o/remove-elem s :a)) s (<? (o/flush! s))
          roots (<? (d/load-roots (:kv-store s) {:sync? sync?}))]
      (is (contains? roots :adds))
      (is (contains? roots :removals)))))

(deftest-async twopset-delta-op-perspective
  (testing "2P-Set δ: add/remove-elem record {:adds}/{:removals} ops; apply-delta ≡ -join"
    (let [s (<? (t/durable-2pset "a" :store-config (mem) :sync? sync?))
          s (<? (t/add s :x)) s (<? (t/add s :y)) s (<? (t/remove-elem s :x))]
      (is (= {:adds #{:x :y} :removals #{:x}} (c/delta-of s)) "δ = the ops, accrued")
      (is (= #{:y} (<? (t/elements s))) "live = adds − removals; δ in meta doesn't affect value")
      (let [peer   (<? (t/durable-2pset "b" :store-config (mem) :sync? sync?))
            peer   (<? (t/add peer :z))
            via-op (<? (c/-apply-delta peer (c/delta-of s)))]
        (is (= #{:y :z} (<? (t/elements via-op))) "ops integrated: :x removed, :y + peer's :z live")
        (is (= (<? (t/elements via-op)) (<? (t/elements (<? (c/-join peer s))))) "op-path ≡ state-path")))))

(deftest-async orset-delta-op-perspective
  (testing "OR-Set δ: add records {:adds #{[elem tag]}} ops; apply-delta ≡ -join"
    (let [oo (<? (o/durable-orset "a" :store-config (mem) :sync? sync?))
          oo (<? (o/add oo :x)) oo (<? (o/add oo :y))
          dl (c/delta-of oo)]
      (is (= #{:x :y} (set (map first (:adds dl)))) "δ :adds carries the [elem tag] add-pairs")
      (let [peer   (<? (o/durable-orset "b" :store-config (mem) :sync? sync?))
            peer   (<? (o/add peer :z))
            via-op (<? (c/-apply-delta peer dl))]
        (is (= #{:x :y :z} (<? (o/elements via-op))) "ops integrated")
        (is (= (<? (o/elements via-op)) (<? (o/elements (<? (c/-join peer oo))))) "op-path ≡ state-path")))))

;; ── JVM-only: file-backend durability across reopen ─────────────────────────────
#?(:clj
   (clojure.test/deftest durable-across-reopen
     (testing "flush, reopen, restore both halves (adds + removals)"
       (let [sc {:backend :file :id (random-uuid) :path (tmpdir)}]
         (-> (o/durable-orset "reg" :store-config sc)
             (o/add :p) (o/add :q) (o/add :r) (o/remove-elem :q) o/flush!)
         (let [re (o/durable-orset "reg" :store-config sc)]
           (is (= #{:p :r} (o/elements re)) "removal persisted across reopen"))))))
