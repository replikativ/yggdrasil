(ns yggdrasil.convergent.gset-test
  "Durable G-Set: convergence laws + cross-store incremental sync + branch-as-
   replica + freeze/isolate + overlay + δ-state. PORTABLE — one body per concern
   runs synchronously on the JVM and as a partial-cps `async` block on cljs (the
   record carries its sync-mode; every async op is wrapped in `<?`).

   VALUE SEMANTICS: every mutator returns a NEW system; tests thread the result
   via `let` rebinding (no in-place mutation, no `->` through async ops).

   Durability runs against a REAL file store on both platforms — `file-cfg` resolves
   to konserve's filestore (JVM) / node-filestore (cljs/node), so flush/restore
   exercise the canonical PSS fressian node handlers everywhere."
  (:require [clojure.test :refer [is testing]]
            [yggdrasil.test-async :refer [deftest-async <? sync? file-cfg]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.gset :as g]
            [yggdrasil.convergent.overlay :as ovl]
            ;; the `<?`/`deftest-async` macros emit `is.simm.partial-cps.async/{await,async}`
            ;; references; in shadow's CommonJS node output the consuming ns must require
            ;; that namespace DIRECTLY for them to resolve (transitive isn't enough).
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [is.simm.partial-cps.async :refer [async]])))

(deftest-async snapshot-id-as-of-and-branch-from-snapshot
  (testing "snapshot-id pins a content root; as-of + branch! re-open it (freeze + run isolated)"
    (let [g0  (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          g0  (<? (g/conj g0 :x))
          g0  (<? (g/conj g0 :y))
          sid (<? (p/snapshot-id g0))                  ; FIX the value {:x :y}
          g   (<? (g/conj g0 :z))]                      ; evolve the live system → {:x :y :z}
      (is (some? sid))
      (is (= #{:x :y :z} (<? (g/elements g))))
      (is (= #{:x :y} (<? (p/as-of g sid))) "as-of re-opens the frozen value")
      ;; branch FROM the snapshot-id → an isolated head at the frozen value
      (let [iso0 (-> g (p/branch! :iso sid) (p/checkout :iso))
            iso  (<? (g/conj iso0 :w))]
        (is (= #{:x :y :w} (<? (g/elements iso))) "isolated branch evolves independently")
        (is (= #{:x :y :z} (<? (g/elements g))) "original main untouched"))))
  (testing "content-addressed: equal content (any order, different store) → equal snapshot-id"
    (let [a0 (<? (g/gset "a" {:store-config (file-cfg)} {:sync? sync?}))
          a0 (<? (g/conj a0 :x)) a0 (<? (g/conj a0 :y))
          a  (<? (p/snapshot-id a0))
          b0 (<? (g/gset "b" {:store-config (file-cfg)} {:sync? sync?}))
          b0 (<? (g/conj b0 :y)) b0 (<? (g/conj b0 :x))
          b  (<? (p/snapshot-id b0))]
      (is (= a b) "snapshot-id is a stable content handle, store-independent"))))

(deftest-async overlay-frozen-isolate-and-merge-down
  (testing "overlay = an isolated clone at the current value; merge-down! joins it back"
    (let [g  (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          g  (<? (g/conj g :x))
          g  (<? (g/conj g :y))
          ov (p/overlay g {:mode :frozen})]
      (<? (ovl/overlay-swap! ov (fn [s] (g/conj s :w))))      ; mutate the overlay in isolation
      (is (= #{:x :y :w} (<? (g/elements (ovl/overlay-system ov)))) "overlay clone evolves independently")
      (is (= #{:x :y}    (<? (g/elements g)))                "parent untouched while overlay is open")
      (let [merged (<? (p/merge-down! ov))]
        (is (= #{:x :y :w} (<? (g/elements merged))) "merge-down! joins the overlay into the parent")))))

(deftest-async overlay-following-isolates-own-writes
  (testing ":following overlay = parent-at-fork JOINED with the overlay's own isolated writes"
    (let [g  (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          g  (<? (g/conj g :x))
          ov (p/overlay g {:mode :following})]
      (is (= :following (:mode ov)) "granted :following (a convergent system can)")
      (<? (ovl/overlay-swap! ov (fn [s] (g/conj s :w))))      ; the overlay's own write (isolated)
      (is (= #{:x :w} (<? (g/elements (<? (ovl/overlay-value ov)))))
          ":following = parent-at-fork (:x) joined with the overlay's :w")
      (is (= #{:x} (<? (g/elements g))) ":w stays isolated to the overlay")))
  (testing ":frozen is pinned at fork time"
    (let [g  (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          g  (<? (g/conj g :x))
          fz (p/overlay g {:mode :frozen})]
      (is (= #{:x} (<? (g/elements (<? (ovl/overlay-value fz))))) ":frozen = the fork snapshot"))))

(deftest-async overlay-discard-leaves-parent-untouched
  (testing "discard! drops the overlay; the parent is unaffected"
    (let [g  (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          g  (<? (g/conj g :a))
          ov (p/overlay g {})]
      (<? (ovl/overlay-swap! ov (fn [s] (g/conj s :b))))
      (is (nil? (p/discard! ov)))
      (is (= #{:a} (<? (g/elements g))) "parent unaffected by a discarded overlay"))))

(deftest-async basic-add-and-read
  (let [a (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
        a (<? (g/conj a :x)) a (<? (g/conj a :y)) a (<? (g/conj a :x))]   ; idempotent add
    (is (= #{:x :y} (<? (g/elements a))))
    (is (true? (c/-conflict-free? a)))
    (is (= :gset (p/system-type a)))))

(deftest-async join-laws-same-store
  (testing "-join is union, commutative, idempotent (pure, same store)"
    (let [r1 (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          r1 (<? (g/conj r1 :a1)) r1 (<? (g/conj r1 :shared))
          r2 (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          r2 (<? (g/conj r2 :b1)) r2 (<? (g/conj r2 :shared))
          ;; -join is pure same-store union; ship r2's nodes into r1's store first
          r1 (<? (g/merge-peer! r1 r2))]
      (is (= #{:a1 :b1 :shared} (<? (g/elements r1))) "union")
      (is (= (<? (g/elements (<? (c/-join r1 r1)))) (<? (g/elements r1))) "idempotent: a⊔a = a"))))

(deftest-async cross-peer-converges-and-ships-incrementally
  (testing "two peers, disjoint adds, ship+merge → union; re-ship copies 0"
    (let [pa (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          pa (<? (g/conj pa :a1)) pa (<? (g/conj pa :a2)) pa (<? (g/flush! pa))
          pb (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          pb (<? (g/conj pb :b1)) pb (<? (g/conj pb :b2)) pb (<? (g/flush! pb))
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
    (let [a (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          a (<? (g/conj a :x)) a (<? (g/conj a :y)) a (<? (g/conj a :z)) a (<? (g/flush! a))
          root  (<? (d/store-set! (get (:roots a) (:current a)) (:storage a) {:sync? sync?}))
          addrs (<? (d/reachable-addresses (:kv-store a) root {:sync? sync?}))]
      (is (contains? addrs root) "root is in the ship-set")
      (is (pos? (count addrs)) "ship-set is non-empty"))))

(deftest-async content-addressed-nodes-dedup-cross-peer
  (testing "two peers build the same set independently → identical root address"
    (let [a (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          a (<? (g/conj a :x)) a (<? (g/conj a :y)) a (<? (g/conj a :z))
          b (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          b (<? (g/conj b :x)) b (<? (g/conj b :y)) b (<? (g/conj b :z))
          ra (<? (d/store-set! (get (:roots a) (:current a)) (:storage a) {:sync? sync?}))
          rb (<? (d/store-set! (get (:roots b) (:current b)) (:storage b) {:sync? sync?}))]
      (is (= ra rb) "identical content → identical content-addressed root")
      (is (zero? (<? (d/ship! (:kv-store b) (:kv-store a) rb {:sync? sync?})))
          "content-addressed → cross-peer dedup, ship is a no-op"))))

(deftest-async branch-is-an-independent-replica
  (testing "branch diverges locally; merge reconverges cleanly (conflict-free)"
    (let [base   (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          base   (<? (g/conj base :seed))
          forked (-> base (p/branch! :fork) (p/checkout :fork))
          forked (<? (g/conj forked :only-fork))]
      (is (= #{:seed :only-fork} (<? (g/elements (p/checkout forked :fork)))) "fork has its own head")
      (is (= #{:seed} (<? (g/elements (p/checkout forked :main)))) "main is unchanged")
      (let [merged (<? (p/merge! (p/checkout forked :main) :fork))]
        (is (= #{:seed :only-fork} (<? (g/elements merged))) "branch-merge is union — no conflict")))))

(deftest-async restore-lazy-set-reads-the-same-elements
  (testing "receive→restore→read: a freshly-restored LAZY storage-backed set drains equal"
    (let [g     (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          g     (<? (g/conj g :x)) g (<? (g/conj g :y)) g (<? (g/flush! g))
          roots (<? (d/load-roots (:kv-store g) {} {:sync? sync?}))
          restored (d/restore-set compare (:main roots) (:storage g) {:sync? sync?})]
      (is (= #{:x :y} (<? (d/set->clj restored {:sync? sync?}))) "lazy restore reads the same elements"))))

(deftest-async delta-state-op-perspective
  (testing "the OP perspective: a mutation records its op as a local δ; apply-delta
            consumes a peer's δ; op-path converges to the SAME value as the state-path"
    (let [g (<? (g/gset "a" {:store-config (file-cfg)} {:sync? sync?}))
          g (<? (g/conj g :x)) g (<? (g/conj g :y))]
      (is (= #{:x :y} (c/delta-of g)) "δ = exactly the ops applied (accrued), no diffing")
      (is (nil? (c/delta-of (c/clear-delta g))) "clear-delta drops the δ")
      (is (= #{:x :y} (<? (g/elements g))) "δ rides in metadata — value/equality unaffected")
      ;; op-path: a peer applies just the δ (cheap, O(δ))
      (let [peer   (<? (g/gset "b" {:store-config (file-cfg)} {:sync? sync?}))
            peer   (<? (g/conj peer :z))
            via-op (<? (g/apply-delta peer (c/delta-of g)))]
        (is (= #{:x :y :z} (<? (g/elements via-op))) "apply-delta unions the peer's ops in")
        (is (nil? (c/delta-of via-op)) "integrating a peer's δ yields a δ-FREE value (no echo)"))
      ;; op-path ≡ state-path: same converged value as a full -join
      (let [peer2 (<? (g/gset "c" {:store-config (file-cfg)} {:sync? sync?}))
            peer2 (<? (g/conj peer2 :z))]
        (is (= (<? (g/elements (<? (g/apply-delta peer2 (c/delta-of g)))))
               (<? (g/elements (<? (c/-join peer2 g)))))
            "op-path (apply δ) ≡ state-path (-join)")))))

;; file-backend durability + co-located cells (portable: JVM filestore / node-filestore)
(deftest-async shared-store-distinct-cells
  (testing "two CRDTs co-habit ONE store under distinct roots/freed cells; each is
            independently durable and restores only its own state (the distinct
            restores prove the cells don't clobber each other)"
    (let [sc (file-cfg)
          a0 (<? (g/gset "a" {:store-config sc
                              :roots-key [:crdt/roots "a"] :freed-key [:crdt/freed "a"]} {:sync? sync?}))
          a1 (<? (g/conj a0 :a1))
          a  (<? (g/conj a1 :a2))
          b0 (<? (g/gset "b" {:kv-store (:kv-store a)
                              :roots-key [:crdt/roots "b"] :freed-key [:crdt/freed "b"]} {:sync? sync?}))
          b  (<? (g/conj b0 :b1))]
      (<? (g/flush! a))
      (<? (g/flush! b))
      (let [a' (<? (g/gset "a" {:store-config sc
                                :roots-key [:crdt/roots "a"] :freed-key [:crdt/freed "a"]} {:sync? sync?}))
            b' (<? (g/gset "b" {:kv-store (:kv-store a')
                                :roots-key [:crdt/roots "b"] :freed-key [:crdt/freed "b"]} {:sync? sync?}))]
        (is (= #{:a1 :a2} (<? (g/elements a'))) "a restores only its own elements")
        (is (= #{:b1} (<? (g/elements b'))) "b restores only its own elements")))))

;; portable file-backed durability (JVM file store + konserve node filestore)
(deftest-async durable-across-reopen
  (testing "flush, reopen the store, restore equals the original set"
    (let [sc (file-cfg)
          g  (<? (g/gset "kb" {:store-config sc} {:sync? sync?}))
          g  (<? (g/conj g :p)) g (<? (g/conj g :q)) g (<? (g/conj g :r))
          _  (<? (g/flush! g))
          reopened (<? (g/gset "kb" {:store-config sc} {:sync? sync?}))]
      (is (= #{:p :q :r} (<? (g/elements reopened))) "restored from disk"))))

;; REGRESSION: reachable-addresses must walk a MULTI-node tree (branch children via
;; node->map, not nil keyword access) — else ship!/merge-peer! copy only the root and
;; the peer can't restore the leaves. Single-leaf sets masked this for a long time.
(deftest-async merge-peer-ships-a-multinode-tree
  (testing "merge-peer! over a 200-element (multi-node) set ships EVERY reachable node"
    (let [elems (set (range 200))
          a (loop [g (<? (g/gset "a" {:store-config (file-cfg)} {:sync? sync?})) es (seq elems)]
              (if es (recur (<? (g/conj g (first es))) (next es)) g))
          a (<? (g/flush! a))
          b (<? (g/gset "b" {:store-config (file-cfg)} {:sync? sync?}))
          b (<? (g/merge-peer! b a))]
      (is (= elems (<? (g/elements b))) "peer received every element (all tree nodes shipped)"))))
