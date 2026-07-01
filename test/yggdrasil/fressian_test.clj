(ns yggdrasil.fressian-test
  "A yggdrasil SYSTEM stored AS A VALUE: the record's plain-data fields serialize
   verbatim and each PSS-backed field serializes as its content-addressed ROOT
   ADDRESS (a reference — dedup via the store's nodes), the way pss-fress serializes
   a set. Reopens with ALL branches + current + dirty intact; the runtime
   storage/kv-store/comparator are re-derived from the read context (reusing the
   SAME `resolve-storage` the PSS handlers use, threaded by `attach-pss-serializer!`).
   Round-trips through the REAL konserve fressian path."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.fressian :as yf]
            [yggdrasil.storage :as storage]
            [yggdrasil.kbridge :as kb]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent.gset :as g]
            [yggdrasil.convergent.cdvcs :as cd]
            [yggdrasil.convergent.twopset :as tps]
            [yggdrasil.convergent.orset :as ors]
            [yggdrasil.convergent.ormap :as orm]
            [yggdrasil.convergent.lwwr :as lwwr]
            [yggdrasil.composite :as cmp]
            [yggdrasil.fn-registry :as fr]
            [yggdrasil.adapters.git :as git]
            [yggdrasil.adapters.datahike :as dha]
            [datahike.api :as d]))

(defn- file-cfg []
  {:backend :file
   :path (str (System/getProperty "java.io.tmpdir") "/ygg-fress-"
              (System/currentTimeMillis) "-" (rand-int 1000000))
   :id (random-uuid)})

(defn- roundtrip
  "k-assoc the system into its OWN store (re-attached with the system value codec,
   whose read handler is handed attach's lexical `resolve-storage`), then k-get it
   back — the real konserve fressian path. Returns the reopened live system."
  [sys]
  (let [store    (:kv-store sys)
        settings (:settings (:storage sys))
        store    (storage/attach-pss-serializer!
                  store settings
                  ;; element-read-handlers as a FUNCTION of the lexical resolver —
                  ;; the system handler reuses the SAME storage resolution as PSS.
                  (fn [resolve-storage]
                    (yf/read-handlers {:resolve-storage resolve-storage :sync? true}))
                  (yf/write-handlers))]
    (kb/k-assoc store :saved/system sys {:sync? true})
    (kb/k-get store :saved/system {:sync? true})))

(deftest gset-record-roundtrips-as-value
  (testing "a G-Set serializes as a value (branch roots → content addresses) and
            reopens with ALL branches + current + dirty preserved"
    (let [base     (-> (g/gset "kb" {:store-config (file-cfg)} {:sync? true})
                       (g/conj :x) (g/conj :y) (g/flush!))      ; main = {:x :y}, dirty cleared
          forked   (-> base (p/branch! :fork) (p/checkout :fork) (g/conj :z))  ; fork = {:x :y :z}
          reopened (roundtrip forked)]
      (is (= :gset (p/system-type reopened)) "reopened as a live G-Set")
      (is (= #{:main :fork} (p/branches reopened)) "BOTH branches survive in the STORE (registry)")
      (is (= :fork (p/current-branch reopened)) "current branch preserved")
      ;; conj AUTO-FLUSHES, so a serialized value is always committed (dirty? clean).
      (is (false? (:dirty? reopened)) "dirty? is clean — the write auto-flushed before serialization")
      (is (= #{:x :y :z} (g/elements (p/checkout reopened :fork))) "fork value reconstructed")
      (is (= #{:x :y}    (g/elements (p/checkout reopened :main))) "main value reconstructed"))))

(deftest cdvcs-record-roundtrips-as-value
  (testing "a CDVCS serializes as a value (graph → content address) and reopens at the
            same head; history works → the graph was restored with graph-cmp (slice
            reads the set's own comparator)"
    (let [a        (cd/cdvcs "doc" {:author "al" :store-config (file-cfg)} {:sync? true})
          a        (cd/commit a "al" [[:assoc :x 1]])
          a        (cd/commit a "al" [[:assoc :y 2]])
          _        (p/commit! a)                       ; flush graph + state cells
          reopened (roundtrip a)]
      (is (= :cdvcs (p/system-type reopened)) "reopened as a live CDVCS")
      (is (= (cd/heads a) (cd/heads reopened)) "same single head")
      (is (= 3 (count (cd/history reopened)))
          "base + 2 commits — history slices the graph with the correct comparator"))))

;; The three flat CRDTs — two-half (adds/removals) PSS records, projected the same
;; way (roots → addresses, restored with `compare`). Generalization proof.

(deftest twopset-roundtrips-as-value
  (testing "2P-Set: add :a :b, remove :b → reopens as {:a}"
    (let [s (-> (tps/twopset "s" {:store-config (file-cfg)} {:sync? true})
                (tps/conj :a) (tps/conj :b) (tps/disj :b))
          reopened (roundtrip s)]
      (is (= :2p-set (p/system-type reopened)))
      (is (= #{:a} (tps/elements reopened)) "tombstone survived the round-trip"))))

(deftest orset-roundtrips-as-value
  (testing "OR-Set: reopens with the live elements (default tag-fn re-injected)"
    (let [o (-> (ors/orset "o" {:store-config (file-cfg)} {:sync? true})
                (ors/conj :x) (ors/conj :y))
          reopened (roundtrip o)]
      (is (= :orset (p/system-type reopened)))
      (is (= #{:x :y} (ors/elements reopened))))))

(deftest ormap-roundtrips-as-value
  (testing "OR-Map: reopens with keys + values; get SLICES the restored graph, so
            the comparator must round-trip correctly"
    (let [m (-> (orm/ormap "m" {:store-config (file-cfg)} {:sync? true})
                (orm/assoc :k1 :v1) (orm/assoc :k2 :v2))
          reopened (roundtrip m)]
      (is (= :ormap (p/system-type reopened)))
      (is (= #{:k1 :k2} (orm/keys reopened)) "keys survive")
      (is (= #{:v1} (orm/get reopened :k1)) "get slices the restored graph correctly"))))

(deftest lwwr-roundtrips-as-value
  (testing "LWW-Register: an IN-MEMORY system (no store of its own) — projects its
            whole value verbatim; vjoin re-injected. Serialized into a HOST store."
    (let [host  (g/gset "host" {:store-config (file-cfg)} {:sync? true})  ; just for a store + settings
          l     (-> (lwwr/lwwr "r") (lwwr/set-register :hello))
          store (storage/attach-pss-serializer!
                 (:kv-store host) (:settings (:storage host))
                 (fn [rs] (yf/read-handlers {:resolve-storage rs :sync? true}))
                 (yf/write-handlers))
          _     (kb/k-assoc store :saved/lwwr l {:sync? true})
          reopened (kb/k-get store :saved/lwwr {:sync? true})]
      (is (= :lwwr (p/system-type reopened)))
      (is (= :hello (lwwr/value reopened)) "register value survives; convergent join still works"))))

(deftest composite-roundtrips-compositionally
  (testing "a composite serializes its WRAPPER only; each co-located child rides its
            OWN ygg/system handler (fressian recurses), so children reconstruct with
            their values automatically — no per-composite child logic"
    (let [sc (file-cfg)
          a  (-> (g/gset "a" {:store-config sc :cell-ns "a"} {:sync? true})
                 (g/conj :a1) (g/conj :a2) (g/flush!))
          b  (-> (g/gset "b" {:kv-store (:kv-store a) :cell-ns "b"} {:sync? true})
                 (g/conj :b1) (g/flush!))
          comp     (cmp/composite [a b] {:store-config sc} {:sync? true})
          reopened (roundtrip comp)]
      (is (= :composite (p/system-type reopened)) "reopened as a composite")
      (let [subs (:systems reopened)]
        (is (= #{"a" "b"} (set (keys subs))) "both child system-ids survive")
        (is (= #{:a1 :a2} (g/elements (clojure.core/get subs "a")))
            "child a reconstructed via its own handler (recursion)")
        (is (= #{:b1} (g/elements (clojure.core/get subs "b"))) "child b reconstructed")))))

(deftest merging-ormap-roundtrips-with-registered-fold
  (testing "a merging-OR-Map's fold rides as a REGISTERED id (the single source of
            truth — runtime fn and serialized id both derive from it). After a
            round-trip get still FOLDS to a scalar (not a value-set), proving the fn
            was resolved from :merge-fn-id, not defaulted to the plain multi-value map"
    (fr/register-fn! :fr-test/sum +)
    (let [m (-> (orm/merging-ormap "mm" :fr-test/sum {:store-config (file-cfg)} {:sync? true})
                (orm/assoc :k 10))
          reopened (roundtrip m)]
      (is (= :merging-ormap (p/system-type reopened)) "stype preserved (merge-fn present)")
      (is (= 10 (orm/get reopened :k))
          "get folds to a SCALAR — the registered (+) fold was recovered from its id, not defaulted to #{10}"))))

(deftest git-roundtrips-as-external-ref
  (testing "a git system serializes its EXTERNAL identity (repo-path + branch); the
            data stays in the repo; reconstruct RECONNECTS via `create` — the external
            flavor (no konserve store, no PSS, no inlined data)"
    (let [path  (str (System/getProperty "java.io.tmpdir") "/ygg-git-"
                     (System/currentTimeMillis) "-" (rand-int 1000000))
          sys   (git/init! path {:system-name "g"})
          rp    (:repo-path sys)
          _     (clojure.java.shell/sh "git" "-C" rp "config" "user.email" "t@t")
          _     (clojure.java.shell/sh "git" "-C" rp "config" "user.name" "t")
          _     (spit (str rp "/f.txt") "hi")
          sys   (p/commit! sys "init")
          ;; git has NO store of its own → serialize into a HOST store
          host  (g/gset "host" {:store-config (file-cfg)} {:sync? true})
          store (storage/attach-pss-serializer!
                 (:kv-store host) (:settings (:storage host))
                 (fn [rs] (yf/read-handlers {:resolve-storage rs :sync? true}))
                 (yf/write-handlers))
          _        (kb/k-assoc store :saved/git sys {:sync? true})
          reopened (kb/k-get store :saved/git {:sync? true})]
      (is (= :git (p/system-type reopened)) "reopened as a git system")
      (is (= rp (:repo-path reopened)) "external identity (repo-path) preserved")
      (is (= (p/snapshot-id sys) (p/snapshot-id reopened))
          "same head SHA — reconstruct reconnected to the LIVE repo"))))

(deftest datahike-roundtrips-by-reconnecting
  (testing "a datahike system serializes its connection CONFIG; reconstruct RECONNECTS
            via datahike's OWN api (it owns the DB codec) — we don't overload datahike,
            the DB is never inlined here"
    (let [cfg  {:store {:backend :memory :id (random-uuid)}
                :keep-history? true :schema-flexibility :read}
          _    (d/create-database cfg)
          conn (d/connect cfg)
          _    (d/transact conn [{:db/id -1 :note "hi"}])
          sys  (dha/create conn {:system-name "db"})
          host (g/gset "host" {:store-config (file-cfg)} {:sync? true})
          store (storage/attach-pss-serializer!
                 (:kv-store host) (:settings (:storage host))
                 (fn [rs] (yf/read-handlers {:resolve-storage rs :sync? true}))
                 (yf/write-handlers))
          _        (kb/k-assoc store :saved/dh sys {:sync? true})
          reopened (kb/k-get store :saved/dh {:sync? true})]
      (is (= :datahike (p/system-type reopened)) "reopened as a datahike system")
      (is (= #{"hi"} (set (map first (d/q '[:find ?n :where [_ :note ?n]] @(:conn reopened)))))
          "reconnected to the SAME datahike DB — data intact"))))
