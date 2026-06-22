(ns yggdrasil.convergent.durable-cdvcs-test
  "Durable CDVCS as a convergent yggdrasil system: durable commit chain + reopen,
   content-addressed snapshot/as-of, the lifted-conflict (-join → multiple heads)
   and its authored resolution (merge), and the decisive CROSS-STORE spike (ship
   the content-addressed blobs, -join the metadata → strong eventual consistency).
   PORTABLE — one .cljc body per concern (JVM sync / cljs async via `<?`)."
  (:require #?(:clj  [clojure.test :refer [is testing]]
               :cljs [cljs.test :refer [is testing]])
            [yggdrasil.test-async :refer [deftest-async <? sync? file-cfg]]
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
          a  (<? (cd/cdvcs "doc" {:author "alice" :store-config sc} {:sync? sync?}))
          a  (<? (cd/commit a "alice" [[:assoc :x 1]]))
          a  (<? (cd/commit a "alice" [[:assoc :y 2]]))
          _  (<? (p/commit! a))]                       ; flush the state cell
      (is (= 1 (count (cd/heads a))) "stays single-head under sequential commits")
      (is (= 3 (count (<? (cd/commit-graph a)))) "base + 2 commits")
      (let [re (<? (cd/cdvcs "doc" {:author "alice" :store-config sc} {:sync? sync?}))]
        (is (= (:state a) (:state re)) "reopen restores the exact convergent state")
        (is (= 3 (count (<? (cd/history re)))) "linear history survives the round-trip")))))

(deftest-async snapshot-and-as-of
  (testing "content-addressed snapshot-id freezes the value; as-of restores it"
    (let [a   (<? (cd/cdvcs "doc" {:author "alice" :store-config (file-cfg)} {:sync? sync?}))
          a   (<? (cd/commit a "alice" [[:assoc :x 1]]))
          sid (<? (p/snapshot-id a))
          a   (<? (cd/commit a "alice" [[:assoc :y 2]]))]
      (is (some? sid))
      (let [frozen (<? (p/as-of a sid))]
        ;; as-of yields the FROZEN handle {:graph <root> :heads :version} taken after
        ;; commit 1 (base + 1 = version 2, single head); the live CDVCS has moved on.
        (is (= 2 (:version frozen)) "as-of restores the frozen version (base + 1)")
        (is (= 1 (count (:heads frozen))) "frozen single head")
        (is (= 3 (count (<? (cd/commit-graph a)))) "the live CDVCS has moved on")))))

(deftest-async divergence-lifts-conflict-then-merge-resolves
  (testing "two lineages in ONE store: -join lifts a 2-head conflict; merge resolves it"
    ;; share one kv-store (distinct state cells); both seed the SAME base commit
    (let [a (<? (cd/cdvcs "a" {:author "root" :store-config (file-cfg)
                               :state-key [:cdvcs/state "a"]} {:sync? sync?}))
          b (<? (cd/cdvcs "b" {:author "root" :kv-store (:kv-store a)
                               :state-key [:cdvcs/state "b"]} {:sync? sync?}))
          a (<? (cd/commit a "alice" [[:assoc :a 1]]))
          b (<? (cd/commit b "bob"   [[:assoc :b 2]]))
          joined (<? (c/-join a b))]
      (is (false? (c/-conflict-free? joined)) "CDVCS lifts conflict — not conflict-free")
      (is (cd/multiple-heads? joined) "concurrent lineages ⇒ a 2-head conflict")
      (is (= 2 (count (cd/heads joined))))
      (let [merged (<? (cd/merge joined "alice" joined))]
        (is (not (cd/multiple-heads? merged)) "merge collapses to a single head")
        (let [merge-id (first (cd/heads merged))]
          (is (= 2 (count (get (<? (cd/commit-graph merged)) merge-id)))
              "the merge commit has BOTH heads as parents"))
        ;; every prior commit is reachable from the merged head (blobs co-located)
        (is (= 4 (count (<? (cd/history merged)))) "base + a + b + merge")))))

(deftest-async cross-store-ship-and-converge
  (testing "TWO separate stores converge: -join the metadata (SEC), ship the
            content-addressed blobs to make every commit readable on both"
    (let [a (<? (cd/cdvcs "x" {:author "al" :store-config (file-cfg)} {:sync? sync?}))
          b (<? (cd/cdvcs "x" {:author "bo" :store-config (file-cfg)} {:sync? sync?}))
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
                  ha   (graph/commit-history (<? (cd/commit-graph a*)) head)
                  hb   (graph/commit-history (<? (cd/commit-graph b*)) head)]
              (is (= ha hb) "the linearisation agrees across peers")
              (is (some? (<? (cd/read-commit a* (first (cd/heads b*))))) "a can now read b's commit")
              (is (some? (<? (cd/read-commit b* (first (cd/heads a*))))) "b can now read a's commit"))))))))

(deftest-async gc-sweep-keeps-live-commits
  (testing "gc-sweep! reclaims superseded graph nodes (prior flushes) yet keeps the
            live commits + linear history readable"
    (let [a (<? (cd/cdvcs "doc" {:author "al" :store-config (file-cfg)} {:sync? sync?}))
          a (<? (p/commit! (<? (cd/commit a "al" [[:assoc :x 1]]))))   ; commit + flush (graph v2)
          a (<? (p/commit! (<? (cd/commit a "al" [[:assoc :x 2]]))))   ; commit + flush (graph v3, supersedes v2)
          a (<? (p/commit! (<? (cd/commit a "al" [[:assoc :x 3]]))))]
      (<? (p/gc-sweep! a nil {:grace-period-ms 0 :sync? sync?}))       ; reclaim everything orphaned before now
      (is (= 4 (count (<? (cd/history a)))) "base + 3 commits survive gc")
      (is (some? (<? (cd/read-commit a (first (cd/heads a))))) "head commit still readable after gc"))))

(deftest-async join-idempotence
  (testing "-join that adds nothing returns the receiver IDENTICAL (no signal re-publish)"
    (let [a (<? (cd/cdvcs "a" {:author "root" :store-config (file-cfg)} {:sync? sync?}))
          a (<? (cd/commit a "al" [[:assoc :x 1]]))
          a (<? (cd/commit a "al" [[:assoc :x 2]]))]
      (is (identical? a (<? (c/-join a a))) "self-join adds nothing ⇒ identical no-op")
      (let [b  (<? (cd/cdvcs "b" {:author "root" :kv-store (:kv-store a)
                                  :state-key [:cdvcs/state "b"]} {:sync? sync?}))
            b  (<? (cd/commit b "bo" [[:assoc :y 1]]))
            j1 (<? (c/-join a b))
            j2 (<? (c/-join j1 b))]
        (is (not (identical? a j1)) "joining a peer with new commits changes the value")
        (is (identical? j1 j2) "re-joining the SAME peer is an identical no-op")))))

(deftest-async durable-pull-fast-forwards
  (testing "pull fast-forwards a behind-peer to the remote tip (durable store-backed LCA)"
    (let [a     (<? (cd/cdvcs "a" {:author "root" :store-config (file-cfg)} {:sync? sync?}))
          b     (<? (cd/cdvcs "b" {:author "root" :kv-store (:kv-store a)
                                   :state-key [:cdvcs/state "b"]} {:sync? sync?}))
          b     (<? (cd/commit b "bo" [[:assoc :y 1]]))     ; b advances ahead of a (shared base)
          b     (<? (cd/commit b "bo" [[:assoc :y 2]]))
          b-tip (first (cd/heads b))
          a'    (<? (cd/pull a b b-tip))]
      (is (= #{b-tip} (cd/heads a')) "a fast-forwarded to b's tip")
      (is (= 3 (count (<? (cd/history a')))) "a now linearises base + b's 2 commits"))))

(deftest-async merge-carries-resolution
  (testing "merge accepts correcting transactions, recorded in the merge commit"
    (let [a      (<? (cd/cdvcs "a" {:author "root" :store-config (file-cfg)} {:sync? sync?}))
          b      (<? (cd/cdvcs "b" {:author "root" :kv-store (:kv-store a)
                                    :state-key [:cdvcs/state "b"]} {:sync? sync?}))
          a      (<? (cd/commit a "al" [[:assoc :k 1]]))
          b      (<? (cd/commit b "bo" [[:assoc :k 2]]))
          joined (<? (c/-join a b))
          merged (<? (cd/merge joined "al" joined [[:assoc :k 3]]))]   ; value-level resolution
      (is (not (cd/multiple-heads? merged)) "merge collapses to one head")
      (let [blob (<? (cd/read-commit merged (first (cd/heads merged))))]
        (is (= [[:assoc :k 3]] (:transactions blob)) "the merge commit carries the resolution txs")))))

(deftest-async delta-op-perspective
  (testing "commit accrues a δ of FULL commits; -apply-delta integrates a peer's δ to
            the SAME value as a full -join (op-path ≡ state-path) — the hook signal-sync
            ships for bidirectional CDVCS over the wire"
    (let [a (<? (cd/cdvcs "a" {:author "root" :store-config (file-cfg)
                               :state-key [:cdvcs/state "a"]} {:sync? sync?}))
          b (<? (cd/cdvcs "b" {:author "root" :kv-store (:kv-store a)
                               :state-key [:cdvcs/state "b"]} {:sync? sync?}))   ; shares a's base
          a (<? (cd/commit a "al" [[:assoc :x 1]]))
          a (<? (cd/commit a "al" [[:assoc :x 2]]))]
      (is (= 2 (count (c/delta-of a))) "δ accrued exactly the two new commits")
      (let [via-op   (<? (c/-apply-delta b (c/delta-of a)))
            via-join (<? (c/-join b a))]
        (is (nil? (c/delta-of via-op)) "apply-delta yields a δ-FREE value (no echo)")
        (is (= (cd/heads via-op) (cd/heads via-join)) "op-path ≡ state-path: identical heads")
        (is (= (cd/heads via-op) (cd/heads a)) "b fast-forwarded to a's head from the δ alone")
        (is (= (count (<? (cd/history via-op))) (count (<? (cd/history a))))
            "b's linear history matches a's after applying the δ")
        ;; idempotent: re-applying the SAME δ adds nothing
        (is (= (cd/heads via-op) (cd/heads (<? (c/-apply-delta via-op (c/delta-of a)))))
            "re-applying the δ is a no-op (idempotent)")))))

(deftest-async delta-concurrent-divergence
  (testing "two peers commit concurrently; each applies the OTHER's δ → both lift the
            same 2-head conflict (δ-path convergence ≡ the -join divergence test)"
    (let [a (<? (cd/cdvcs "a" {:author "root" :store-config (file-cfg)
                               :state-key [:cdvcs/state "a"]} {:sync? sync?}))
          b (<? (cd/cdvcs "b" {:author "root" :kv-store (:kv-store a)
                               :state-key [:cdvcs/state "b"]} {:sync? sync?}))
          a  (<? (cd/commit a "al" [[:assoc :a 1]]))
          b  (<? (cd/commit b "bo" [[:assoc :b 2]]))
          a* (<? (c/-apply-delta a (c/delta-of b)))      ; a integrates b's commit
          b* (<? (c/-apply-delta b (c/delta-of a)))]     ; b integrates a's commit
      (is (= (cd/heads a*) (cd/heads b*)) "both converge on the same 2-head frontier (SEC)")
      (is (= 2 (count (cd/heads a*))) "concurrent lineages ⇒ a lifted 2-head conflict"))))

(deftest-async gc-retains-named-snapshot
  (testing "gc-sweep! with a retained snapshot-id keeps that frozen version restorable"
    (let [a   (<? (cd/cdvcs "a" {:author "al" :store-config (file-cfg)} {:sync? sync?}))
          a   (<? (cd/commit a "al" [[:assoc :x 1]]))
          sid (<? (p/snapshot-id a))                         ; freeze here
          a   (<? (p/commit! (<? (cd/commit a "al" [[:assoc :x 2]]))))]
      (<? (p/gc-sweep! a [sid] {:grace-period-ms 0 :sync? sync?}))   ; retain the named snapshot
      (is (some? (<? (p/as-of a sid))) "the retained snapshot survives gc")
      (is (= 3 (count (<? (cd/history a)))) "live history intact"))))
