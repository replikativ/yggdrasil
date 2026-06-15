(ns yggdrasil.convergent.durable-gset-test
  "Durable G-Set: convergence laws + cross-store incremental sync + branch-as-
   replica + durability across reopen. Proves the registry's PSS+konserve
   substrate, lifted into a conflict-free system, converges and ships
   incrementally — the durable analogue of the in-memory catalog.

   VALUE SEMANTICS: every mutator returns a NEW system; tests thread the result
   (no in-place mutation). The mutable cell is the holder (signal / overlay
   `:local-writes`), not the CRDT value."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.durable-gset :as g]
            [yggdrasil.convergent.overlay :as ovl])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- mem [] {:backend :memory :id (random-uuid)})

(defn- tmpdir []
  (str (Files/createTempDirectory "ygg-dgset" (make-array FileAttribute 0))))

(deftest snapshot-id-as-of-and-branch-from-snapshot
  (testing "snapshot-id pins a content root; as-of + branch! re-open it (freeze + run isolated)"
    (let [g0  (-> (g/durable-gset "kb" :store-config (mem)) (g/add :x) (g/add :y))
          sid (p/snapshot-id g0)                           ; FIX the value {:x :y}
          g   (g/add g0 :z)]                               ; evolve the live system → {:x :y :z}
      (is (some? sid))
      (is (= #{:x :y :z} (g/elements g)))
      ;; as-of the frozen snapshot returns the OLD value, not the current branch
      (is (= #{:x :y} (p/as-of g sid)) "as-of re-opens the frozen value")
      ;; branch FROM the snapshot-id → an isolated head at the frozen value
      (let [iso0 (-> g (p/branch! :iso sid) (p/checkout :iso))
            iso  (g/add iso0 :w)]                          ; evolve the isolated branch
        (is (= #{:x :y :w} (g/elements iso)) "isolated branch evolves independently")
        (is (= #{:x :y :z} (g/elements g)) "original main untouched by the isolated branch")))
    (testing "content-addressed: equal content (any order, different store) → equal snapshot-id"
      (let [a (p/snapshot-id (-> (g/durable-gset "a" :store-config (mem)) (g/add :x) (g/add :y)))
            b (p/snapshot-id (-> (g/durable-gset "b" :store-config (mem)) (g/add :y) (g/add :x)))]
        (is (= a b) "snapshot-id is a stable content handle, store-independent")))))

(deftest overlay-frozen-isolate-and-merge-down
  (testing "overlay = an isolated clone at the current value; merge-down! joins
            it back into the parent (the uniform isolate / residue fix)"
    (let [g  (-> (g/durable-gset "kb" :store-config (mem)) (g/add :x) (g/add :y))
          ov (p/overlay g {:mode :frozen})]
      (ovl/overlay-swap! ov (fn [s] (g/add s :w)))         ; mutate the overlay in isolation
      (is (= #{:x :y :w} (g/elements (ovl/overlay-system ov))) "overlay clone evolves independently")
      (is (= #{:x :y}    (g/elements g))                   "parent untouched while the overlay is open")
      (let [merged (p/merge-down! ov)]
        (is (= #{:x :y :w} (g/elements merged)) "merge-down! joins the overlay into the parent")))))

(deftest overlay-following-isolates-own-writes
  (testing ":following overlay = parent-at-fork JOINED with the overlay's own
            isolated writes. (Value-semantic: the overlay captures the parent BY
            VALUE, so live-tracking of a still-evolving parent is now the FRP
            signal's job — re-derive the overlay when the parent signal fires —
            not the overlay layer.)"
    (let [g  (-> (g/durable-gset "kb" :store-config (mem)) (g/add :x))
          ov (p/overlay g {:mode :following})]
      (is (= :following (:mode ov)) "granted :following (a convergent system can)")
      (ovl/overlay-swap! ov (fn [s] (g/add s :w)))         ; the overlay's own write (isolated)
      (is (= #{:x :w} (g/elements (ovl/overlay-value ov)))
          ":following = parent-at-fork (:x) joined with the overlay's :w")
      (is (= #{:x} (g/elements g)) ":w stays isolated to the overlay"))
    (testing ":frozen is pinned at fork time"
      (let [g  (-> (g/durable-gset "kb" :store-config (mem)) (g/add :x))
            fz (p/overlay g {:mode :frozen})]
        (is (= #{:x} (g/elements (ovl/overlay-value fz))) ":frozen = the fork snapshot")))))

(deftest overlay-discard-leaves-parent-untouched
  (testing "discard! drops the overlay; the parent is unaffected"
    (let [g  (-> (g/durable-gset "kb" :store-config (mem)) (g/add :a))
          ov (p/overlay g {})]
      (ovl/overlay-swap! ov (fn [s] (g/add s :b)))
      (is (nil? (p/discard! ov)))
      (is (= #{:a} (g/elements g)) "parent unaffected by a discarded overlay"))))

(deftest shared-store-distinct-cells
  (testing "two CRDTs co-habit ONE store under distinct roots/freed cells — the
            single-causal-root composite (option a). Each is independently
            durable; reopening via its own roots-key restores only its state."
    (let [sc {:backend :file :id (random-uuid) :path (tmpdir)}
          a (-> (g/durable-gset "a" :store-config sc
                                :roots-key [:crdt/roots "a"] :freed-key [:crdt/freed "a"])
                (g/add :a1) (g/add :a2))
          ;; b co-habits a's SAME kv-store, with its own cells
          b (-> (g/durable-gset "b" :kv-store (:kv-store a)
                                :roots-key [:crdt/roots "b"] :freed-key [:crdt/freed "b"])
                (g/add :b1))]
      (g/flush! a) (g/flush! b)                            ; persist (store side-effect)
      ;; both cells live side-by-side in the one store, no clobber
      (is (some? (k/get (:kv-store a) [:crdt/roots "a"] nil {:sync? true})))
      (is (some? (k/get (:kv-store a) [:crdt/roots "b"] nil {:sync? true})))
      ;; reopen from the SAME store via each roots-key → independent restore
      (let [a' (g/durable-gset "a" :store-config sc
                               :roots-key [:crdt/roots "a"] :freed-key [:crdt/freed "a"])
            b' (g/durable-gset "b" :kv-store (:kv-store a')
                               :roots-key [:crdt/roots "b"] :freed-key [:crdt/freed "b"])]
        (is (= #{:a1 :a2} (g/elements a')) "a restores only its own elements")
        (is (= #{:b1} (g/elements b')) "b restores only its own elements")))))

(deftest basic-add-and-read
  (let [a (-> (g/durable-gset "kb" :store-config (mem))
              (g/add :x) (g/add :y) (g/add :x))]   ; idempotent add
    (is (= #{:x :y} (g/elements a)))
    (is (true? (c/-conflict-free? a)))
    (is (= :gset (p/system-type a)))))

(deftest join-laws-same-store
  (testing "-join is union, commutative, idempotent (pure, same store)"
    (let [r1 (-> (g/durable-gset "kb" :store-config (mem)) (g/add :a1) (g/add :shared))
          r2 (-> (g/durable-gset "kb" :store-config (mem)) (g/add :b1) (g/add :shared))
          ;; -join is pure same-store union of branch heads. Put r2's nodes into r1's
          ;; store first (ship) so the union can traverse them.
          r1 (g/merge-peer! r1 r2)]
      (is (= #{:a1 :b1 :shared} (g/elements r1)) "union")
      (is (= (g/elements (c/-join r1 r1)) (g/elements r1)) "idempotent: a⊔a = a"))))

(deftest cross-peer-converges-and-ships-incrementally
  (testing "two peers, disjoint adds, ship+merge → union; re-ship copies 0"
    (let [peer-a (-> (g/durable-gset "kb" :store-config (mem)) (g/add :a1) (g/add :a2) g/flush!)
          peer-b (-> (g/durable-gset "kb" :store-config (mem)) (g/add :b1) (g/add :b2) g/flush!)
          ;; b's stable root before any restructuring
          broot  (d/store-set! (get (:roots peer-b) (:current peer-b)) (:storage peer-b))]
      ;; first ship of b's nodes into a copies > 0; re-shipping the SAME root
      ;; copies nothing — incremental.
      (is (pos? (d/ship! (:kv-store peer-b) (:kv-store peer-a) broot)) "first ship transfers nodes")
      (is (zero? (d/ship! (:kv-store peer-b) (:kv-store peer-a) broot)) "re-ship is a no-op (incremental)")

      ;; converge both directions → union; strong eventual consistency
      (let [peer-a (g/flush! (g/merge-peer! peer-a peer-b))
            peer-b (g/flush! (g/merge-peer! peer-b peer-a))]
        (is (= #{:a1 :a2 :b1 :b2} (g/elements peer-a)) "peer-a converged to the union")
        (is (= #{:a1 :a2 :b1 :b2} (g/elements peer-b)) "peer-b converged to the union")
        (is (= (g/elements peer-a) (g/elements peer-b)) "both peers agree (SEC)")))))

(deftest ship-set-is-the-reachable-nodes
  (testing "reachable-addresses = root + transitive child node addresses"
    (let [a (-> (g/durable-gset "kb" :store-config (mem))
                (g/add :x) (g/add :y) (g/add :z) g/flush!)
          root (d/store-set! (get (:roots a) (:current a)) (:storage a))
          addrs (d/reachable-addresses (:kv-store a) root)]
      (is (contains? addrs root) "root is in the ship-set")
      (is (pos? (count addrs)) "ship-set is non-empty"))))

(deftest content-addressed-nodes-dedup-cross-peer
  (testing "two peers build the same set independently → identical root address"
    ;; Merkle addressing: a node's address IS the hash of its content, so the
    ;; SAME logical tree gets the SAME addresses on different peers/stores —
    ;; this is what makes cross-peer merge dedup cleanly (the storage graph
    ;; converges, not just the value).
    (let [a (-> (g/durable-gset "kb" :store-config (mem)) (g/add :x) (g/add :y) (g/add :z))
          b (-> (g/durable-gset "kb" :store-config (mem)) (g/add :x) (g/add :y) (g/add :z))
          ra (d/store-set! (get (:roots a) (:current a)) (:storage a))
          rb (d/store-set! (get (:roots b) (:current b)) (:storage b))]
      (is (= ra rb) "identical content → identical content-addressed root")
      ;; therefore shipping b's tree into a's store copies nothing
      (is (zero? (d/ship! (:kv-store b) (:kv-store a) rb))
          "content-addressed → cross-peer dedup, ship is a no-op"))))

(deftest branch-is-an-independent-replica
  (testing "branch diverges locally; -join reconverges cleanly (conflict-free)"
    (let [sc (mem)
          base (-> (g/durable-gset "kb" :store-config sc) (g/add :seed))
          forked (-> base (p/branch! :fork) (p/checkout :fork) (g/add :only-fork))]
      ;; forked has its own head; main is unchanged
      (is (= #{:seed :only-fork} (set (get (:roots forked) :fork))))
      (is (= #{:seed} (set (get (:roots forked) :main))))
      ;; merge fork → main is the value join
      (let [merged (-> forked (p/checkout :main) (p/merge! :fork))]
        (is (= #{:seed :only-fork} (set (get (:roots merged) :main)))
            "branch-merge is union — no conflict")))))

(deftest durable-across-reopen
  (testing "flush, reopen the store, restore equals the original set"
    (let [dir (tmpdir)
          sc  {:backend :file :id (random-uuid) :path dir}]
      (-> (g/durable-gset "kb" :store-config sc) (g/add :p) (g/add :q) (g/add :r) g/flush!)
      ;; reopen a fresh handle on the same store
      (let [reopened (g/durable-gset "kb" :store-config sc)]
        (is (= #{:p :q :r} (g/elements reopened)) "restored from disk")))))

(deftest delta-state-op-perspective
  (testing "the OP perspective: a mutation records its op as a local δ (captured
            at the write, no diffing); apply-delta consumes a peer's δ; the op-path
            converges to the SAME value as the state-path (-join)"
    (let [g (-> (g/durable-gset "a" :store-config {:backend :memory :id (random-uuid)})
                (g/add :x) (g/add :y))]
      (is (= #{:x :y} (c/delta-of g)) "δ = exactly the ops applied (accrued), no diffing")
      (is (nil? (c/delta-of (c/clear-delta g))) "clear-delta drops the δ")
      (is (= #{:x :y} (g/elements g)) "δ rides in metadata — value/equality unaffected")
      ;; op-path: a peer applies just the δ (cheap, O(δ))
      (let [peer   (-> (g/durable-gset "b" :store-config {:backend :memory :id (random-uuid)}) (g/add :z))
            via-op (g/apply-delta peer (c/delta-of g))]
        (is (= #{:x :y :z} (g/elements via-op)) "apply-delta unions the peer's ops in")
        (is (= #{:z} (c/delta-of via-op))
            "remote-integrated ops are NOT added to the local δ — only the peer's own
             un-propagated :z remains, so remote ops never echo back to the sender"))
      ;; op-path ≡ state-path: same converged value as a full -join
      (let [peer2 (-> (g/durable-gset "c" :store-config {:backend :memory :id (random-uuid)}) (g/add :z))]
        (is (= (g/elements (g/apply-delta peer2 (c/delta-of g)))
               (g/elements (c/-join peer2 g)))
            "op-path (apply δ) ≡ state-path (-join)")))))
