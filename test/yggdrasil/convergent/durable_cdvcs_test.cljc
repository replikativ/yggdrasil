(ns yggdrasil.convergent.durable-cdvcs-test
  "Durable CDVCS as a convergent yggdrasil system: durable commit chain + reopen,
   content-addressed snapshot/as-of, the lifted-conflict (-join → multiple heads)
   and its authored resolution (merge), and the decisive CROSS-STORE spike (ship
   the content-addressed blobs, -join the metadata → strong eventual consistency).
   PORTABLE — one .cljc body per concern (JVM sync / cljs async via `<?`)."
  (:require #?(:clj  [clojure.test :refer [is testing]]
               :cljs [cljs.test :refer [is testing]])
            [yggdrasil.test-async :refer [deftest-async <? sync? mem file-cfg]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.cdvcs :as cd]
            [yggdrasil.convergent.cdvcs.graph :as graph]
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [is.simm.partial-cps.async :refer [async]])))

(deftest-async durable-commit-chain-reopen
  (testing "commits chain durably; flush + reopen restores the convergent value"
    (let [sc (file-cfg)
          a  (<? (cd/cdvcs "doc" :author "alice" :store-config sc :sync? sync?))
          a  (<? (cd/commit a "alice" [[:assoc :x 1]]))
          a  (<? (cd/commit a "alice" [[:assoc :y 2]]))
          _  (<? (p/commit! a))]                       ; flush the state cell
      (is (= 1 (count (cd/heads a))) "stays single-head under sequential commits")
      (is (= 3 (count (cd/commit-graph a))) "base + 2 commits")
      (let [re (<? (cd/cdvcs "doc" :author "alice" :store-config sc :sync? sync?))]
        (is (= (:state a) (:state re)) "reopen restores the exact convergent state")
        (is (= 3 (count (cd/history re))) "linear history survives the round-trip")))))

(deftest-async snapshot-and-as-of
  (testing "content-addressed snapshot-id freezes the value; as-of restores it"
    (let [a   (<? (cd/cdvcs "doc" :author "alice" :store-config (mem) :sync? sync?))
          a   (<? (cd/commit a "alice" [[:assoc :x 1]]))
          sid (<? (p/snapshot-id a))
          a   (<? (cd/commit a "alice" [[:assoc :y 2]]))]
      (is (some? sid))
      (let [frozen (<? (p/as-of a sid))]
        (is (= 2 (count (:commit-graph frozen))) "as-of restores the FROZEN state (base + 1)")
        (is (= 3 (count (cd/commit-graph a))) "the live CDVCS has moved on")))))

(deftest-async divergence-lifts-conflict-then-merge-resolves
  (testing "two lineages in ONE store: -join lifts a 2-head conflict; merge resolves it"
    ;; share one kv-store (distinct state cells); both seed the SAME base commit
    (let [a (<? (cd/cdvcs "a" :author "root" :store-config (mem) :sync? sync?
                          :state-key [:cdvcs/state "a"]))
          b (<? (cd/cdvcs "b" :author "root" :kv-store (:kv-store a) :sync? sync?
                          :state-key [:cdvcs/state "b"]))
          a (<? (cd/commit a "alice" [[:assoc :a 1]]))
          b (<? (cd/commit b "bob"   [[:assoc :b 2]]))
          joined (<? (c/-join a b))]
      (is (false? (c/-conflict-free? joined)) "CDVCS lifts conflict — not conflict-free")
      (is (cd/multiple-heads? joined) "concurrent lineages ⇒ a 2-head conflict")
      (is (= 2 (count (cd/heads joined))))
      (let [merged (<? (cd/merge joined "alice" joined))]
        (is (not (cd/multiple-heads? merged)) "merge collapses to a single head")
        (let [merge-id (first (cd/heads merged))]
          (is (= 2 (count (get (cd/commit-graph merged) merge-id)))
              "the merge commit has BOTH heads as parents"))
        ;; every prior commit is reachable from the merged head (blobs co-located)
        (is (= 4 (count (cd/history merged))) "base + a + b + merge")))))

(deftest-async cross-store-ship-and-converge
  (testing "TWO separate stores converge: -join the metadata (SEC), ship the
            content-addressed blobs to make every commit readable on both"
    (let [a (<? (cd/cdvcs "x" :author "al" :store-config (file-cfg) :sync? sync?))
          b (<? (cd/cdvcs "x" :author "bo" :store-config (file-cfg) :sync? sync?))
          a (<? (cd/commit a "al" [[:assoc :x 1]]))
          b (<? (cd/commit b "bo" [[:assoc :y 2]]))]
      (testing "-join converges on metadata ALONE (no blobs needed yet)"
        (let [a* (<? (c/-join a b))
              b* (<? (c/-join b a))]
          (is (= (:state a*) (:state b*)) "strong eventual consistency: identical state")
          (is (= 2 (count (cd/heads a*))) "both lineages present as heads")
          (testing "after shipping the missing blobs, full history is readable on both"
            (<? (cd/ship! a (:kv-store b)))          ; a's commit → b's store
            (<? (cd/ship! b (:kv-store a)))          ; b's commit → a's store
            ;; pick one head and linearise it on each side — same commit set
            (let [head (first (cd/heads a*))
                  ha   (graph/commit-history (cd/commit-graph a*) head)
                  hb   (graph/commit-history (cd/commit-graph b*) head)]
              (is (= ha hb) "the linearisation agrees across peers")
              (is (some? (<? (cd/read-commit a* (first (cd/heads b*))))) "a can now read b's commit")
              (is (some? (<? (cd/read-commit b* (first (cd/heads a*))))) "b can now read a's commit"))))))))
