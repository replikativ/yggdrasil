(ns yggdrasil.composite-cljs-test
  "Runtime proof that the CompositeSystem runs on ClojureScript over ASYNC
   konserve. The composite carries its sync-mode (`:sync? false`), so every
   method touching sub-systems or persistence (snapshot-id / commit! / merge! /
   as-of) is async (CPS you `await`), while in-memory history reads
   (snapshot-meta) stay sync. Proves the transactional commit (flush subs, write
   `:composite/root` last) on the browser path, with two durable-CRDT subs
   co-located in ONE store (the lone-causal-root form)."
  (:require [cljs.test :refer-macros [deftest is async]]
            [cljs.core.async :as a :refer [go <!]]
            [yggdrasil.protocols :as p]
            ;; the CompositeSystem `-join` extension lives in yggdrasil.composite now
            ;; (logic proven on JVM by composite-test). TODO: a cljs peer -join
            ;; assertion (PARITY gap).
            [yggdrasil.composite :as cmp]
            [yggdrasil.convergent.gset :as g]))

(defn- realize [cps]
  (let [ch (a/promise-chan)]
    (cps (fn [v] (a/put! ch [:ok v])) (fn [e] (a/put! ch [:err e])))
    ch))

(defn- gset-opener
  "An opener that co-locates a durable G-Set in the composite's store under its
   own [:crdt/roots id] cell."
  [id]
  (fn [kv o]
    (g/gset id {:kv-store kv :roots-key [:crdt/roots id]} {:sync? (:sync? o)})))

(deftest composite-async-transactional-end-to-end
  (async done
         (go
           (let [sc           {:backend :memory :id (random-uuid)}
            ;; the composite opened :sync? false → its sub-touching methods are async
                 [tc comp]    (<! (realize (cmp/composite [(gset-opener "a") (gset-opener "b")]
                                                          {:store-config sc} {:sync? false})))
            ;; value-semantic: evolve subs via update-subsystem (re-seats the new
            ;; sub value into the composite), threading the composite each step.
                 [ta comp]    (<! (realize (cmp/update-subsystem comp "a" #(g/conj % :a1))))
                 [_ comp]     (<! (realize (cmp/update-subsystem comp "a" #(g/conj % :a2))))
                 [_ comp]     (<! (realize (cmp/update-subsystem comp "b" #(g/conj % :b1))))
            ;; transactional commit: flush both subs durable, write :composite/root LAST
                 [tcommit committed] (<! (realize (p/commit! comp "snap-1")))
                 [tsid sid]   (<! (realize (p/snapshot-id committed)))
                 meta         (p/snapshot-meta committed sid)        ; SYNC (in-memory map) on cljs too
            ;; as-of resolves each sub through the bundle + restores it on cljs
            ;; (via PSS `restore`) — freeze+isolate works cross-platform now.
                 [taf as-of]  (<! (realize (p/as-of committed sid)))]
             (is (= :ok tc) "composite constructed async")
             (is (= :ok ta) "co-located sub add is async")
             (is (= :ok tcommit) "transactional commit is async")
             (is (= :ok tsid))
             (is (some? sid) "committed composite has a snapshot id")
             (is (= #{"a" "b"} (set (keys (:sub-snapshots meta))))
                 "the committed bundle references every sub-system (sync map read)")
             (is (= :ok taf) "as-of restores each sub on cljs")
             (is (= #{:a1 :a2} (get as-of "a")) "as-of resolves sub a through the bundle")
             (is (= #{:b1} (get as-of "b")) "as-of resolves sub b through the bundle")
             (done)))))
