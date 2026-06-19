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

(deftest-async save-roots-is-a-grow-map
  (testing "save-roots! merges, never clobbers a branch it doesn't know"
    (let [gs (<? (g/gset "t" :store-config (file-cfg) :sync? sync?))
          kv (:kv-store gs)]
      (<? (d/save-roots! kv {:main :ra} {:sync? sync?}))
      (<? (d/save-roots! kv {:feature :rf} {:sync? sync?}))        ; a writer that only knows :feature
      (is (= {:main :ra :feature :rf} (<? (d/load-roots kv {:sync? sync?}))) ":main survived")
      (<? (d/save-roots! kv {:main :ra2} {:sync? sync?}))          ; shared branch: incoming wins
      (is (= {:main :ra2 :feature :rf} (<? (d/load-roots kv {:sync? sync?})))))))

(deftest-async multi-branch-peers-converge
  (testing "two peers each add a distinct branch → merge keeps BOTH branches"
    (let [a (<? (g/gset "kb" :store-config (file-cfg) :sync? sync?))
          a (<? (g/conj a :shared))
          a (-> a (p/branch! :fa) (p/checkout :fa))
          a (<? (g/conj a :a-only))
          a (<? (g/flush! a))
          b (<? (g/gset "kb" :store-config (file-cfg) :sync? sync?))
          b (<? (g/conj b :shared))
          b (-> b (p/branch! :fb) (p/checkout :fb))
          b (<? (g/conj b :b-only))
          b (<? (g/flush! b))
          ;; converge both directions (value-semantic: thread the merged handles)
          a (<? (g/flush! (<? (g/merge-peer! a b))))
          b (<? (g/flush! (<? (g/merge-peer! b a))))]
      ;; every branch present on both peers (none lost to LWW)
      (is (= #{:main :fa :fb} (p/branches a)))
      (is (= #{:main :fa :fb} (p/branches b)))
      (is (= (p/branches a) (p/branches b)))
      ;; the branch-only elements survived on both
      (is (= #{:shared :a-only} (<? (g/elements (p/checkout a :fa)))))
      (is (= #{:shared :b-only} (<? (g/elements (p/checkout b :fb)))))
      ;; and the roots cell on disk reflects all three branches
      (is (= #{:main :fa :fb} (set (keys (<? (d/load-roots (:kv-store a) {:sync? sync?})))))))))
