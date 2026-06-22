(ns yggdrasil.fressian-test
  "A yggdrasil SYSTEM stored AS A VALUE: it serializes to a thin content-addressed
   snapshot reference (the `ygg/system` Fressian tag) and reopens from the store with
   the same value — the system-level analog of pss-fress's `pss/set`. Round-trips
   through the REAL konserve fressian path (a file store re-attached with the system
   handlers via `attach-pss-serializer!`'s element-handler extension point)."
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
  "k-assoc the system into its OWN store (re-attached with the system handlers
   alongside the PSS node handlers), then k-get it back — the real konserve fressian
   path. Returns the reopened live system."
  [sys]
  (let [store    (:kv-store sys)
        settings (:settings (:storage sys))
        store    (storage/attach-pss-serializer!
                  store settings
                  (yf/read-handlers {:resolve-store (constantly store) :sync? true})
                  (yf/write-handlers))]
    (kb/k-assoc store :saved/system sys {:sync? true})
    (kb/k-get store :saved/system {:sync? true})))

(deftest gset-system-roundtrips-as-snapshot-reference
  (testing "a G-Set stored as a value reopens from its snapshot handle + the store's nodes"
    (let [gs       (-> (g/gset "kb" {:store-config (file-cfg)} {:sync? true})
                       (g/conj :x) (g/conj :y) (g/flush!))
          reopened (roundtrip gs)]
      (is (= :gset (p/system-type reopened)) "reopened as a live G-Set")
      (is (= #{:x :y} (g/elements reopened))
          "value reconstructed from the thin handle — only a reference crossed the codec"))))

(deftest cdvcs-system-roundtrips-as-snapshot-reference
  (testing "a CDVCS stored as a value reopens at the same head + full history"
    (let [a        (cd/cdvcs "doc" {:author "al" :store-config (file-cfg)} {:sync? true})
          a        (cd/commit a "al" [[:assoc :x 1]])
          a        (cd/commit a "al" [[:assoc :y 2]])
          _        (p/commit! a)                       ; flush graph + state cells
          reopened (roundtrip a)]
      (is (= :cdvcs (p/system-type reopened)) "reopened as a live CDVCS")
      (is (= (cd/heads a) (cd/heads reopened)) "reopened at the same single head")
      (is (= 3 (count (cd/history reopened)))
          "base + 2 commits restored from the snapshot graph root"))))
