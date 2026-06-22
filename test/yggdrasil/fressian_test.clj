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
            [yggdrasil.convergent.cdvcs :as cd]))

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
          forked   (-> base (p/branch! :fork) (p/checkout :fork) (g/conj :z))  ; fork = {:x :y :z}, UNFLUSHED
          reopened (roundtrip forked)]
      (is (= :gset (p/system-type reopened)) "reopened as a live G-Set")
      (is (= #{:main :fork} (p/branches reopened)) "BOTH branches survive (not just one snapshot)")
      (is (= :fork (p/current-branch reopened)) "current branch preserved")
      (is (= #{:fork} (:dirty reopened)) "dirty set preserved verbatim")
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
