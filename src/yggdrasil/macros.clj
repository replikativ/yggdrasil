(ns ^:no-doc yggdrasil.macros
  "Sync/async duality for the konserve-backed storage layer, via partial-cps —
   the same substrate spindel and persistent-sorted-set use (so a yggdrasil
   storage op is `await`-able inside a spin, with value semantics + no channel
   overhead, instead of thread-blocking it).

   `async+sync` mirrors persistent-sorted-set's macro: one body compiles to a
   synchronous form on JVM (`{:sync? true}` — `async`/`await` collapse to `do`,
   no CPS, no channels) and to the partial-cps async form on cljs
   (`{:sync? false}`). Used from `.cljc` storage/registry/composite namespaces
   via `#?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]])
          :clj  (:require        [yggdrasil.macros :refer [async+sync]]))`."
  (:require [clojure.walk :as walk]))

(def ^:private async->sync
  "Rewrite the partial-cps async breakpoints to their synchronous no-ops."
  '{is.simm.partial-cps.async/async do
    is.simm.partial-cps.async/await do
    async do
    await do})

(defmacro async+sync
  "If `sync?`, evaluate `async-code` with `async`/`await` rewritten to `do`
   (synchronous, no CPS); otherwise evaluate it as the partial-cps async form."
  [sync? async-code]
  (let [sync-code (walk/postwalk (fn [n] (get async->sync n n)) async-code)]
    `(if ~sync? ~sync-code ~async-code)))
