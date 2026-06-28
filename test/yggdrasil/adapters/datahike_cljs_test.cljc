(ns yggdrasil.adapters.datahike-cljs-test
  "Cross-platform COMPILE guard for the datahike adapter (.clj → .cljc): the ns
   must compile on cljs — no leaked JVM interop, reader conditionals correct, and
   datahike/konserve/superv.async APIs all resolve on cljs. The adapter's full
   JVM behaviour is covered by datahike_test.clj; full datahike RUNTIME on cljs
   (create/transact/gc-storage!/listen against a node store) is exercised by the
   real cljs consumer (simmis), not here.

   Note: gc-sweep! is async+sync — it blocking-derefs on the JVM and returns a
   superv.async channel on cljs; listen rides datahike.core/listen! (cljc)."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.adapters.datahike :as dha]))

(deftest adapter-compiles-and-exposes-surface
  (testing "the datahike adapter ns compiles on this platform + exposes its API"
    (is (fn? dha/create) "public constructor present")
    (is (some? dha/->DatahikeSystem) "DatahikeSystem record constructor present")))
