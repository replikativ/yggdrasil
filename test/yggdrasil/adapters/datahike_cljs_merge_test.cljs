(ns yggdrasil.adapters.datahike-cljs-merge-test
  "CLJS RUNTIME guard for the datahike adapter on node — the full fork → write →
   merge path, which had never run on ClojureScript before (JVM behaviour:
   datahike_test.clj + spindel's workspace_fork_e2e_test.clj; the only cljs coverage
   was a COMPILE-only surface guard in datahike_cljs_test.cljc).

   The adapter's storage-touching ops (branch!/checkout/merge!) are now on yggdrasil's
   native partial-cps substrate: on cljs each yields an `await`-able CPS. So this test
   drives them with yggdrasil's partial-cps `deftest-async`/`<?`; datahike's own
   (core.async) ops are bridged into the same substrate with the core.async adapter's
   `chan->cps`. JVM-only: datahike_test.clj."
  (:require [cljs.test :refer-macros [is testing]]
            [cljs.test]
            [yggdrasil.test-async]                ; runtime: loads partial-cps substrate
            [is.simm.partial-cps.core-async :as ca]
            [datahike.api :as d]
            [yggdrasil.adapters.datahike :as dha]
            [yggdrasil.protocols :as p])
  (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]))

(defn- item-names [conn]
  (set (map first (d/q '[:find ?n :where [_ :item/name ?n]] @conn))))

(deftest-async fork-write-merge-runtime-on-cljs
  (testing "fork (branch!+checkout), isolated write, and merge! all run on cljs/node
            via the partial-cps substrate; merge! yields a SYSTEM (peer-symmetric with
            the JVM) carrying both datoms"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? true :schema-flexibility :read}]
      ;; datahike's own ops are core.async on cljs → bridge into partial-cps via chan->cps
      (<? (ca/chan->cps (d/create-database cfg)))
      (let [conn (<? (ca/chan->cps (d/connect cfg {:sync? false})))]
        (<? (ca/chan->cps (d/transact! conn {:tx-data [{:db/ident :item/name
                                                        :db/valueType :db.type/string
                                                        :db/cardinality :db.cardinality/one}]})))
        (<? (ca/chan->cps (d/transact! conn {:tx-data [{:item/name "trunk"}]})))
        (let [sys (dha/create conn {:system-name "kb"})]
          ;; adapter ops are now partial-cps CPS on cljs → await them directly
          (<? (p/branch! sys :feat))
          (let [feat (<? (p/checkout sys :feat))]
            (is (= :feat (p/current-branch feat)) "checkout lands on the fork branch")
            (<? (ca/chan->cps (d/transact! (:conn feat) {:tx-data [{:item/name "fork-edit"}]})))
            (is (= #{"trunk"} (item-names conn))
                "parent branch is isolated from the fork write")
            (let [merged (<? (p/merge! sys :feat))]
              (is (instance? dha/DatahikeSystem merged)
                  "cljs merge! yields a DatahikeSystem (not a tx-report / channel)")
              (is (= #{"trunk" "fork-edit"} (item-names (:conn merged)))
                  "the merged parent branch carries BOTH the trunk + fork datoms"))))))))
