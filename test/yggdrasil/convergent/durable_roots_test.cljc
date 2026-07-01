(ns yggdrasil.convergent.durable-roots-test
  "Convergent root cell: the :crdt/roots branch→root map is a grow-map (merge on
   write), and merge-peer! converges EVERY branch — so two peers that each add a
   different branch don't lose one under blind LWW. PORTABLE — one body per concern
   runs synchronously on the JVM and as a partial-cps `async` block on cljs (every
   async durable op is wrapped in `<?`; branch!/checkout/branches are plain sync)."
  (:require [clojure.test :refer [is testing]]
            [yggdrasil.test-async :refer [deftest-async <? sync? file-cfg]]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.gset :as g]
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [is.simm.partial-cps.async :refer [async]])))

(deftest-async branches-registry-is-a-grow-set
  (testing "register-branch! is a grow-set UNION — never clobbers a branch a concurrent
            writer (or synced peer) added between read and write (TOCTOU-safe)"
    (let [gs (<? (g/gset "t" {:store-config (file-cfg)} {:sync? sync?}))
          kv (:kv-store gs)]
      (<? (d/register-branch! kv :main {} {:sync? sync?}))
      (<? (d/register-branch! kv :feature {} {:sync? sync?}))    ; a writer that only knows :feature
      (is (= #{:main :feature} (<? (d/load-branches kv {} {:sync? sync?}))) ":main survived")
      (<? (d/register-branch! kv :main {} {:sync? sync?}))       ; idempotent re-add
      (is (= #{:main :feature} (<? (d/load-branches kv {} {:sync? sync?})))))))

(deftest-async multi-branch-peers-converge
  (testing "two peers each add a distinct branch → merge keeps BOTH branches"
    (let [a (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          a (<? (g/conj a :shared))
          a (<? (p/checkout (<? (p/branch! a :fa)) :fa {:sync? sync?}))
          a (<? (g/conj a :a-only))
          a (<? (g/flush! a))
          b (<? (g/gset "kb" {:store-config (file-cfg)} {:sync? sync?}))
          b (<? (g/conj b :shared))
          b (<? (p/checkout (<? (p/branch! b :fb)) :fb {:sync? sync?}))
          b (<? (g/conj b :b-only))
          b (<? (g/flush! b))
          ;; converge both directions (merge-peer! is a whole-tree cross-store reconcile)
          a (<? (g/flush! (<? (g/merge-peer! a b))))
          b (<? (g/flush! (<? (g/merge-peer! b a))))]
      ;; every branch present on both peers (none lost to LWW)
      (is (= #{:main :fa :fb} (<? (p/branches a {:sync? sync?}))))
      (is (= #{:main :fa :fb} (<? (p/branches b {:sync? sync?}))))
      (is (= (<? (p/branches a {:sync? sync?})) (<? (p/branches b {:sync? sync?}))))
      ;; the branch-only elements survived on both
      (is (= #{:shared :a-only} (<? (g/elements (<? (p/checkout a :fa {:sync? sync?}))))))
      (is (= #{:shared :b-only} (<? (g/elements (<? (p/checkout b :fb {:sync? sync?}))))))
      ;; and the branch registry on disk reflects all three branches
      (is (= #{:main :fa :fb} (<? (d/load-branches (:kv-store a) {} {:sync? sync?})))))))
