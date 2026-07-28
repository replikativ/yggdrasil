(ns yggdrasil.adapters.geschichte-cljs-test
  "Cross-platform COMPILE guard for the Geschichte adapter: the ns must compile
   on cljs — no leaked JVM interop, reader conditionals correct, and every
   geschichte API it uses must resolve on cljs.

   This matters more than for the other adapters: the whole point of keeping the
   adapter `.cljc` is a browser-side review/editor surface over a room repo, so
   a JVM-only leak here is a silent regression of that goal. Every geschichte ns
   the adapter touches (`repo` `query` `diff` `workspace` `merge.core` `bytes`
   `async`) is itself `.cljc`; the JVM-only `geschichte.merge` is deliberately
   not used.

   Full JVM behaviour is covered by geschichte_test.clj. Full geschichte RUNTIME
   on cljs (a datahike node store under the repo) is exercised by the real cljs
   consumer, not here — same split as the datahike adapter."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.adapters.geschichte :as gy]
            [yggdrasil.types :as t]))

(deftest adapter-compiles-and-exposes-surface
  (testing "the geschichte adapter ns compiles on this platform + exposes its API"
    (is (fn? gy/create) "public constructor present")
    (is (some? gy/->GeschichteSystem) "GeschichteSystem record constructor present")
    (is (some? gy/->GeschichteOverlay) "GeschichteOverlay record constructor present")
    (is (fn? gy/connection) "virtual-filesystem seam present")
    (is (fn? gy/workspace-id) "workspace identity present")))

(deftest geschichte-diff-type-is-available-on-both-platforms
  (testing "the diff record consumers destructure is portable"
    (let [d (t/->GeschichteDiff "a" "b" "" "" [{:status :added :path "x"}] {})]
      (is (= "a" (:snapshot-a d)))
      (is (= [{:status :added :path "x"}] (:files d))
          "leading fields mirror GitDiff so renderers need no special case"))))
