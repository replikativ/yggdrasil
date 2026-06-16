(ns yggdrasil.convergent.durable-gset-test
  "Durable G-Set: convergence laws + cross-store incremental sync + branch-as-
   replica + freeze/isolate + overlay + δ-state. PORTABLE — one body per concern
   runs synchronously on the JVM and as a partial-cps `async` block on cljs (the
   record carries its sync-mode; every async op is wrapped in `<?`).

   VALUE SEMANTICS: every mutator returns a NEW system; tests thread the result
   via `let` rebinding (no in-place mutation, no `->` through async ops).

   File-backend durability/reopen tests are JVM-only (`#?(:clj …)`) — node
   konserve has no equivalent local file store in this harness."
  (:require [clojure.test :refer [is testing]]
            [yggdrasil.test-async :refer [deftest-async <? sync? mem file-cfg]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-gset :as g]
            [yggdrasil.convergent.overlay :as ovl]
            ;; the `<?`/`deftest-async` macros emit `is.simm.partial-cps.async/{await,async}`
            ;; references; in shadow's CommonJS node output the consuming ns must require
            ;; that namespace DIRECTLY for them to resolve (transitive isn't enough).
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime])
            #?(:clj [konserve.core :as k]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [is.simm.partial-cps.async :refer [async]]))
  #?(:clj (:import [java.nio.file Files]
                   [java.nio.file.attribute FileAttribute])))

#?(:clj (defn- tmpdir []
          (str (Files/createTempDirectory "ygg-dgset" (make-array FileAttribute 0)))))

(deftest-async snapshot-id-as-of-and-branch-from-snapshot
  (testing "snapshot-id pins a content root; as-of + branch! re-open it (freeze + run isolated)"
    (let [g0  (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          g0  (<? (g/add g0 :x))
          g0  (<? (g/add g0 :y))
          sid (<? (p/snapshot-id g0))                  ; FIX the value {:x :y}
          g   (<? (g/add g0 :z))]                      ; evolve the live system → {:x :y :z}
      (is (some? sid))
      (is (= #{:x :y :z} (<? (g/elements g))))
      (is (= #{:x :y} (<? (p/as-of g sid))) "as-of re-opens the frozen value")
      ;; branch FROM the snapshot-id → an isolated head at the frozen value
      (let [iso0 (-> g (p/branch! :iso sid) (p/checkout :iso))
            iso  (<? (g/add iso0 :w))]
        (is (= #{:x :y :w} (<? (g/elements iso))) "isolated branch evolves independently")
        (is (= #{:x :y :z} (<? (g/elements g))) "original main untouched"))))
  (testing "content-addressed: equal content (any order, different store) → equal snapshot-id"
    (let [a0 (<? (g/durable-gset "a" :store-config (mem) :sync? sync?))
          a0 (<? (g/add a0 :x)) a0 (<? (g/add a0 :y))
          a  (<? (p/snapshot-id a0))
          b0 (<? (g/durable-gset "b" :store-config (mem) :sync? sync?))
          b0 (<? (g/add b0 :y)) b0 (<? (g/add b0 :x))
          b  (<? (p/snapshot-id b0))]
      (is (= a b) "snapshot-id is a stable content handle, store-independent"))))

(deftest-async overlay-frozen-isolate-and-merge-down
  (testing "overlay = an isolated clone at the current value; merge-down! joins it back"
    (let [g  (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          g  (<? (g/add g :x))
          g  (<? (g/add g :y))
          ov (p/overlay g {:mode :frozen})]
      (<? (ovl/overlay-swap! ov (fn [s] (g/add s :w))))      ; mutate the overlay in isolation
      (is (= #{:x :y :w} (<? (g/elements (ovl/overlay-system ov)))) "overlay clone evolves independently")
      (is (= #{:x :y}    (<? (g/elements g)))                "parent untouched while overlay is open")
      (let [merged (<? (p/merge-down! ov))]
        (is (= #{:x :y :w} (<? (g/elements merged))) "merge-down! joins the overlay into the parent")))))

(deftest-async overlay-following-isolates-own-writes
  (testing ":following overlay = parent-at-fork JOINED with the overlay's own isolated writes"
    (let [g  (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          g  (<? (g/add g :x))
          ov (p/overlay g {:mode :following})]
      (is (= :following (:mode ov)) "granted :following (a convergent system can)")
      (<? (ovl/overlay-swap! ov (fn [s] (g/add s :w))))      ; the overlay's own write (isolated)
      (is (= #{:x :w} (<? (g/elements (<? (ovl/overlay-value ov)))))
          ":following = parent-at-fork (:x) joined with the overlay's :w")
      (is (= #{:x} (<? (g/elements g))) ":w stays isolated to the overlay")))
  (testing ":frozen is pinned at fork time"
    (let [g  (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          g  (<? (g/add g :x))
          fz (p/overlay g {:mode :frozen})]
      (is (= #{:x} (<? (g/elements (<? (ovl/overlay-value fz))))) ":frozen = the fork snapshot"))))

(deftest-async overlay-discard-leaves-parent-untouched
  (testing "discard! drops the overlay; the parent is unaffected"
    (let [g  (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          g  (<? (g/add g :a))
          ov (p/overlay g {})]
      (<? (ovl/overlay-swap! ov (fn [s] (g/add s :b))))
      (is (nil? (p/discard! ov)))
      (is (= #{:a} (<? (g/elements g))) "parent unaffected by a discarded overlay"))))

(deftest-async basic-add-and-read
  (let [a (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
        a (<? (g/add a :x)) a (<? (g/add a :y)) a (<? (g/add a :x))]   ; idempotent add
    (is (= #{:x :y} (<? (g/elements a))))
    (is (true? (c/-conflict-free? a)))
    (is (= :gset (p/system-type a)))))

(deftest-async join-laws-same-store
  (testing "-join is union, commutative, idempotent (pure, same store)"
    (let [r1 (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          r1 (<? (g/add r1 :a1)) r1 (<? (g/add r1 :shared))
          r2 (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          r2 (<? (g/add r2 :b1)) r2 (<? (g/add r2 :shared))
          ;; -join is pure same-store union; ship r2's nodes into r1's store first
          r1 (<? (g/merge-peer! r1 r2))]
      (is (= #{:a1 :b1 :shared} (<? (g/elements r1))) "union")
      (is (= (<? (g/elements (<? (c/-join r1 r1)))) (<? (g/elements r1))) "idempotent: a⊔a = a"))))

(deftest-async cross-peer-converges-and-ships-incrementally
  (testing "two peers, disjoint adds, ship+merge → union; re-ship copies 0"
    (let [pa (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          pa (<? (g/add pa :a1)) pa (<? (g/add pa :a2)) pa (<? (g/flush! pa))
          pb (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          pb (<? (g/add pb :b1)) pb (<? (g/add pb :b2)) pb (<? (g/flush! pb))
          broot (<? (d/store-set! (get (:roots pb) (:current pb)) (:storage pb) {:sync? sync?}))]
      (is (pos? (<? (d/ship! (:kv-store pb) (:kv-store pa) broot {:sync? sync?}))) "first ship transfers nodes")
      (is (zero? (<? (d/ship! (:kv-store pb) (:kv-store pa) broot {:sync? sync?}))) "re-ship is a no-op (incremental)")
      (let [pa (<? (g/merge-peer! pa pb)) pa (<? (g/flush! pa))
            pb (<? (g/merge-peer! pb pa)) pb (<? (g/flush! pb))]
        (is (= #{:a1 :a2 :b1 :b2} (<? (g/elements pa))) "peer-a converged to the union")
        (is (= #{:a1 :a2 :b1 :b2} (<? (g/elements pb))) "peer-b converged to the union")
        (is (= (<? (g/elements pa)) (<? (g/elements pb))) "both peers agree (SEC)")))))

(deftest-async ship-set-is-the-reachable-nodes
  (testing "reachable-addresses = root + transitive child node addresses"
    (let [a (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          a (<? (g/add a :x)) a (<? (g/add a :y)) a (<? (g/add a :z)) a (<? (g/flush! a))
          root  (<? (d/store-set! (get (:roots a) (:current a)) (:storage a) {:sync? sync?}))
          addrs (<? (d/reachable-addresses (:kv-store a) root {:sync? sync?}))]
      (is (contains? addrs root) "root is in the ship-set")
      (is (pos? (count addrs)) "ship-set is non-empty"))))

(deftest-async content-addressed-nodes-dedup-cross-peer
  (testing "two peers build the same set independently → identical root address"
    (let [a (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          a (<? (g/add a :x)) a (<? (g/add a :y)) a (<? (g/add a :z))
          b (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          b (<? (g/add b :x)) b (<? (g/add b :y)) b (<? (g/add b :z))
          ra (<? (d/store-set! (get (:roots a) (:current a)) (:storage a) {:sync? sync?}))
          rb (<? (d/store-set! (get (:roots b) (:current b)) (:storage b) {:sync? sync?}))]
      (is (= ra rb) "identical content → identical content-addressed root")
      (is (zero? (<? (d/ship! (:kv-store b) (:kv-store a) rb {:sync? sync?})))
          "content-addressed → cross-peer dedup, ship is a no-op"))))

(deftest-async branch-is-an-independent-replica
  (testing "branch diverges locally; merge reconverges cleanly (conflict-free)"
    (let [base   (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          base   (<? (g/add base :seed))
          forked (-> base (p/branch! :fork) (p/checkout :fork))
          forked (<? (g/add forked :only-fork))]
      (is (= #{:seed :only-fork} (<? (g/elements (p/checkout forked :fork)))) "fork has its own head")
      (is (= #{:seed} (<? (g/elements (p/checkout forked :main)))) "main is unchanged")
      (let [merged (<? (p/merge! (p/checkout forked :main) :fork))]
        (is (= #{:seed :only-fork} (<? (g/elements merged))) "branch-merge is union — no conflict")))))

(deftest-async restore-lazy-set-reads-the-same-elements
  (testing "receive→restore→read: a freshly-restored LAZY storage-backed set drains equal"
    (let [g     (<? (g/durable-gset "kb" :store-config (mem) :sync? sync?))
          g     (<? (g/add g :x)) g (<? (g/add g :y)) g (<? (g/flush! g))
          roots (<? (d/load-roots (:kv-store g) {:sync? sync?}))
          restored (d/restore-set compare (:main roots) (:storage g) {:sync? sync?})]
      (is (= #{:x :y} (<? (d/set->clj restored {:sync? sync?}))) "lazy restore reads the same elements"))))

(deftest-async delta-state-op-perspective
  (testing "the OP perspective: a mutation records its op as a local δ; apply-delta
            consumes a peer's δ; op-path converges to the SAME value as the state-path"
    (let [g (<? (g/durable-gset "a" :store-config (mem) :sync? sync?))
          g (<? (g/add g :x)) g (<? (g/add g :y))]
      (is (= #{:x :y} (c/delta-of g)) "δ = exactly the ops applied (accrued), no diffing")
      (is (nil? (c/delta-of (c/clear-delta g))) "clear-delta drops the δ")
      (is (= #{:x :y} (<? (g/elements g))) "δ rides in metadata — value/equality unaffected")
      ;; op-path: a peer applies just the δ (cheap, O(δ))
      (let [peer   (<? (g/durable-gset "b" :store-config (mem) :sync? sync?))
            peer   (<? (g/add peer :z))
            via-op (<? (g/apply-delta peer (c/delta-of g)))]
        (is (= #{:x :y :z} (<? (g/elements via-op))) "apply-delta unions the peer's ops in")
        (is (nil? (c/delta-of via-op)) "integrating a peer's δ yields a δ-FREE value (no echo)"))
      ;; op-path ≡ state-path: same converged value as a full -join
      (let [peer2 (<? (g/durable-gset "c" :store-config (mem) :sync? sync?))
            peer2 (<? (g/add peer2 :z))]
        (is (= (<? (g/elements (<? (g/apply-delta peer2 (c/delta-of g)))))
               (<? (g/elements (<? (c/-join peer2 g)))))
            "op-path (apply δ) ≡ state-path (-join)")))))

;; ── JVM-only: file-backend durability + co-located cells (node konserve has no
;; equivalent local file store in this harness) ─────────────────────────────────
#?(:clj
   (clojure.test/deftest shared-store-distinct-cells
     (testing "two CRDTs co-habit ONE store under distinct roots/freed cells; each
               is independently durable and restores only its own state"
       (let [sc {:backend :file :id (random-uuid) :path (tmpdir)}
             a (-> (g/durable-gset "a" :store-config sc
                                   :roots-key [:crdt/roots "a"] :freed-key [:crdt/freed "a"])
                   (g/add :a1) (g/add :a2))
             b (-> (g/durable-gset "b" :kv-store (:kv-store a)
                                   :roots-key [:crdt/roots "b"] :freed-key [:crdt/freed "b"])
                   (g/add :b1))]
         (g/flush! a) (g/flush! b)
         (is (some? (k/get (:kv-store a) [:crdt/roots "a"] nil {:sync? true})))
         (is (some? (k/get (:kv-store a) [:crdt/roots "b"] nil {:sync? true})))
         (let [a' (g/durable-gset "a" :store-config sc
                                  :roots-key [:crdt/roots "a"] :freed-key [:crdt/freed "a"])
               b' (g/durable-gset "b" :kv-store (:kv-store a')
                                  :roots-key [:crdt/roots "b"] :freed-key [:crdt/freed "b"])]
           (is (= #{:a1 :a2} (g/elements a')) "a restores only its own elements")
           (is (= #{:b1} (g/elements b')) "b restores only its own elements"))))))

;; portable file-backed durability (JVM file store + konserve node filestore)
(deftest-async durable-across-reopen
  (testing "flush, reopen the store, restore equals the original set"
    (let [sc (file-cfg)
          g  (<? (g/durable-gset "kb" :store-config sc :sync? sync?))
          g  (<? (g/add g :p)) g (<? (g/add g :q)) g (<? (g/add g :r))
          _  (<? (g/flush! g))
          reopened (<? (g/durable-gset "kb" :store-config sc :sync? sync?))]
      (is (= #{:p :q :r} (<? (g/elements reopened))) "restored from disk"))))
