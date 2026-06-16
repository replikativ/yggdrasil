(ns yggdrasil.convergent.cdvcs-test
  "The PURE CDVCS heart: the commit-graph join (downstream) is commutative /
   associative / idempotent; LCA + head recomputation; commit/merge/pull verbs;
   the lifted-conflict (multiple-heads) state and its explicit resolution. No
   store, no async — portable plain deftest on BOTH platforms."
  (:require #?(:clj  [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [clojure.set :as set]
            [yggdrasil.types :as t]
            [yggdrasil.convergent.cdvcs.graph :as graph]
            [yggdrasil.convergent.cdvcs.core :as c]))

;; deterministic, monotonic clock so commit ids/order are stable in the test
(defn- with-clock [f]
  (let [n (atom 0)]
    (binding [t/*now-fn* (fn [] (swap! n inc))]
      (f))))

;; ---- pure graph algebra (no clock needed) ----

(deftest downstream-is-a-join
  (testing "downstream merges commit-graphs and recomputes heads; it is a join"
    ;; a linear chain 0←1, then two divergent children 2 and 3 of head 1
    (let [a {:commit-graph {0 [] 1 [0] 2 [1]} :heads #{2} :version 1}
          b {:commit-graph {0 [] 1 [0] 3 [1]} :heads #{3} :version 1}]
      (testing "commutative + yields the lifted conflict (two heads)"
        (is (= (graph/downstream a b) (graph/downstream b a)))
        (is (= #{2 3} (:heads (graph/downstream a b))))
        (is (= {0 [] 1 [0] 2 [1] 3 [1]} (:commit-graph (graph/downstream a b)))))
      (testing "idempotent: joining with self is a no-op"
        (is (= (graph/downstream a a) a)))
      (testing "associative"
        (let [cc {:commit-graph {0 [] 1 [0] 4 [1]} :heads #{4} :version 1}]
          (is (= (graph/downstream (graph/downstream a b) cc)
                 (graph/downstream a (graph/downstream b cc)))))))))

(deftest fast-forward-join-collapses-to-one-head
  (testing "joining an ancestor into a descendant keeps the single descendant head"
    (let [old {:commit-graph {0 [] 1 [0]} :heads #{1} :version 1}
          new {:commit-graph {0 [] 1 [0] 2 [1]} :heads #{2} :version 1}]
      (is (= #{2} (:heads (graph/downstream old new))))
      (is (= #{2} (:heads (graph/downstream new old)))))))

(deftest lca-and-history
  (testing "lowest-common-ancestor of two divergent heads is their fork point"
    (let [g {0 [] 1 [0] 2 [1] 3 [1]}]
      (is (= #{1} (:lcas (graph/lowest-common-ancestors g #{2} g #{3}))))))
  (testing "commit-history is a depth-first linearisation, each commit once"
    (let [g {0 [] 1 [0] 2 [1] 3 [2]}]
      (is (= [0 1 2 3] (graph/commit-history g 3))))))

;; ---- value verbs ----

(deftest commit-builds-linear-history
  (with-clock
    (fn []
      (testing "single-head commits chain; history linearises"
        (let [{s0 :state} (c/new-cdvcs "alice")
              {s1 :state} (c/commit s0 "alice" [[:assoc :x 1]])
              {s2 :state} (c/commit s1 "alice" [[:assoc :y 2]])]
          (is (= 1 (count (:heads s0))))
          (is (= 1 (count (:heads s2))) "stays single-head under sequential commits")
          (is (= 3 (count (:commit-graph s2))) "base + 2 commits")
          (is (= 3 (count (graph/commit-history (:commit-graph s2)
                                                (first (:heads s2)))))))))))

(deftest divergence-then-merge
  (with-clock
    (fn []
      (testing "two replicas commit on the same base → join lifts a 2-head conflict;
                merge resolves it into one head via a merge commit"
        (let [{base :state}    (c/new-cdvcs "root")
              {a :state ca :commits} (c/commit base "alice" [[:assoc :a 1]])
              {b :state cb :commits} (c/commit base "bob"   [[:assoc :b 2]])
              joined            (graph/downstream a b)]
          (is (c/multiple-heads? joined) "concurrent commits ⇒ lifted conflict")
          (is (= 2 (count (:heads joined))))
          (let [{m :state cm :commits} (c/merge joined "alice" joined)]
            (is (not (c/multiple-heads? m)) "merge collapses to a single head")
            (is (= 1 (count (:heads m))))
            (let [merge-id (first (:heads m))]
              (is (= 2 (count (get-in m [:commit-graph merge-id])))
                  "the merge commit has BOTH heads as parents"))
            (is (= 1 (count cm)) "merge produced exactly one new (merge) commit blob")
            ;; every commit is reachable from the merged head
            (let [hist (set (graph/commit-history (:commit-graph m) (first (:heads m))))]
              (is (every? hist (concat (keys ca) (keys cb)))))))))))

(deftest pull-fast-forwards-and-refuses-conflict
  (with-clock
    (fn []
      (testing "pull a remote tip that descends from us = fast-forward"
        (let [{base :state}      (c/new-cdvcs "root")
              {remote :state}    (c/commit base "bob" [[:assoc :z 9]])
              remote-tip         (first (:heads remote))
              {pulled :state}    (c/pull base remote remote-tip)]
          (is (= #{remote-tip} (:heads pulled)) "fast-forwarded to the remote tip")))
      (testing "pull that would induce a conflict throws (use merge instead)"
        (let [{base :state}   (c/new-cdvcs "root")
              {a :state}      (c/commit base "alice" [[:assoc :a 1]])
              {b :state}      (c/commit base "bob"   [[:assoc :b 2]])
              btip            (first (set/difference (:heads b) (:heads base)))]
          (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core/ExceptionInfo)
                       (c/pull a b btip))))))))
