(ns yggdrasil.test-async
  "Cross-platform `deftest-async` over yggdrasil's partial-cps `async+sync`
   substrate — ONE test body runs synchronously on the JVM (durable ops opened
   `:sync? true` return values) and as a single partial-cps `async` block on
   ClojureScript (every durable op suspends on konserve IO and yields a CPS).

   This is the test-suite analogue of `yggdrasil.macros/async+sync`: the test
   author writes one body, wrapping every ASYNC durable op in `<?`. On the JVM
   `<?` is identity (the op already returned its value); on cljs `<?` is the
   partial-cps `await` (valid because the whole body is wrapped in `async`).

   It is the partial-cps cousin of datahike's core.async `deftest-async`: same
   compile-time platform dispatch, but built on the SAME substrate the durable
   layer uses (no core.async / no `realize`→promise-chan bridge).

   Usage (a portable `.cljc` test ns):

       (ns yggdrasil.convergent.gset-test
         (:require [clojure.test :refer [is testing]]
                   [yggdrasil.test-async :refer [deftest-async <? sync? mem]]
                   [yggdrasil.convergent.gset :as g])
         #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]])))

       (deftest-async roundtrip
         (let [g   (<? (g/gset \"t\" :store-config (mem) :sync? sync?))
               g   (<? (g/conj g :x))
               els (<? (g/elements g))]
           (is (= #{:x} els))))

   RULES:
   - Wrap ONLY genuinely async ops (those written with `async+sync`) in `<?`.
     Wrapping a plain-sync op breaks cljs — `await` on a non-CPS value errors.
   - Thread `sync?` into the durable FACTORY's `:sync?`; subsequent ops inherit
     the mode from the record's `:opts`. Pass `{:sync? sync?}` to any op called
     with explicit opts (e.g. `d/load-roots`)."
  #?(:clj  (:require [clojure.test]
                     [is.simm.partial-cps.async])
     :cljs (:require [cljs.test]
                     [is.simm.partial-cps.async]
                     ;; registers konserve's `:file` backend defmethods on node, so
                     ;; `{:backend :file}` resolves via `kstore/connect-store` /
                     ;; `create-store` / `store-exists?` exactly like the JVM file store.
                     [konserve.node-filestore]))
  #?(:cljs (:require-macros [yggdrasil.test-async]
                            [is.simm.partial-cps.async]))
  #?(:clj (:import [java.nio.file Files]
                   [java.nio.file.attribute FileAttribute])))

(defn- cljs-env? [env] (some? (:ns env)))

(def sync?
  "Platform default sync-mode for durable ops: `true` (blocking values) on the
   JVM, `false` (partial-cps CPS) on cljs. Thread into a durable factory's
   `:sync?` so the record carries the right mode."
  #?(:clj true :cljs false))

(defn mem
  "A fresh in-memory konserve store-config — the portable backend for tests."
  []
  {:backend :memory :id (random-uuid)})

(defn file-cfg
  "A fresh FILE-backed konserve store-config in a unique temp dir — portable:
   the JVM file store and konserve's node filestore (registered via the cljs
   require above) both back `{:backend :file …}` through the lifecycle
   multimethods. Use for durability-across-reopen tests on BOTH platforms.
   Durable ops over it run async on cljs (`:sync? sync?` → false) — wrap in `<?`."
  []
  {:backend :file
   :id (random-uuid)
   :path #?(:clj  (str (Files/createTempDirectory "ygg-file" (make-array FileAttribute 0)))
            :cljs (str (.tmpdir (js/require "os")) "/ygg-file-" (random-uuid)))})

(defmacro <?
  "Resolve an async durable op portably. On cljs, `await` the partial-cps CPS
   (valid only inside a `deftest-async` body's `async` block). On the JVM the op
   already returned its value (sync mode) so this is identity. Wrap ONLY async
   ops — see the ns docstring."
  [x]
  (if (cljs-env? &env)
    `(is.simm.partial-cps.async/await ~x)
    x))

(defmacro deftest-async
  "Like `clojure.test/deftest`, but the body may use `<?` to resolve async
   durable (partial-cps) ops uniformly. On the JVM the body runs synchronously in
   a plain `deftest` (`<?` is identity). On cljs the body is driven as ONE
   partial-cps `async` block under `cljs.test/async`, so each `<?` suspends on
   konserve IO and the test completes via `done`."
  [tname & body]
  (if (cljs-env? &env)
    `(cljs.test/deftest ~tname
       (cljs.test/async done#
                        ;; `cljs.test/run-block` chains the NEXT test block synchronously
                        ;; through this `done` continuation (its `@d`). When our blocks
                        ;; resolve synchronously (in-memory store), `done#` fires from
                        ;; INSIDE this block's partial-cps trampoline, where
                        ;; `*in-trampoline*` is true — so the next block would enter as a
                        ;; fresh top-level async invocation but skip establishing its own
                        ;; forcing loop, and run-block discards the Thunk it returns →
                        ;; dropped continuation → hang. Reset `*in-trampoline*` to false at
                        ;; this boundary (we are handing control back to a foreign driver)
                        ;; so the next block self-trampolines. (partial-cps contract: a
                        ;; foreign caller must either trampoline the returned Thunk or
                        ;; invoke with `*in-trampoline*` false.)
                        (let [done!# (fn []
                                       (binding [is.simm.partial-cps.async/*in-trampoline* false]
                                         (done#)))]
                          ((is.simm.partial-cps.async/async
                            (try ~@body
                                 (catch :default e#
                                   (cljs.test/is false (str "deftest-async threw: "
                                                            (or (.-message e#) e#) "\n"
                                                            (.-stack e#))))))
                           (fn [_#] (done!#))
                           (fn [e#]
                             (cljs.test/is false (str "deftest-async rejected: "
                                                      (or (.-message e#) e#) "\n"
                                                      (when (and e# (.-stack e#)) (.-stack e#))))
                             (done!#))))))
    `(clojure.test/deftest ~tname ~@body)))
