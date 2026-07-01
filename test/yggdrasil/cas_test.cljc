(ns yggdrasil.cas-test
  "Content-addressed break-out: break-out! + resolve round-trip, the HashRef pointer
   surviving the store serializer inside a node element, and hash transparency."
  (:require [clojure.test :refer [is testing]]
            [yggdrasil.test-async :refer [deftest-async <? sync? file-cfg]]
            [yggdrasil.cas :as cas]
            [yggdrasil.kbridge :as kb]
            [yggdrasil.convergent.durable :as d]
            [hasch.core :as hasch]
            #?(:cljs [is.simm.partial-cps.async])
            #?(:cljs [is.simm.partial-cps.runtime]))
  #?(:cljs (:require-macros [yggdrasil.test-async :refer [deftest-async <?]]
                            [is.simm.partial-cps.async :refer [async]])))

(deftest-async break-out-resolve-and-transparency
  (testing "break-out! stores a value under its content-address + returns a HashRef; resolve
            fetches it; the ref round-trips through the store serializer INSIDE a node
            element (the P0 that fails on a wrong/absent handler); break-out is transparent
            to the enclosing content-hash."
    (let [{:keys [kv-store]} (<? (d/open (file-cfg) {} {:sync? sync?}))
          kv  kv-store
          v   {:big (vec (range 300)) :note "a broken-out payload"}
          ref (<? (cas/break-out! kv v {:sync? sync?}))]
      (is (= v (<? (cas/resolve kv ref {:sync? sync?}))) "break-out! + resolve round-trip")
      ;; store the ref inside a node-like value THROUGH the serializer, restore, resolve
      (<? (kb/k-assoc kv :test-node {:level 0 :keys [{:elem ref} :plain]} nil {:sync? sync?}))
      (let [back     (<? (kb/k-get kv :test-node {:sync? sync?}))
            back-ref (:elem (first (:keys back)))]
        (is (= v (<? (cas/resolve kv back-ref {:sync? sync?})))
            "the HashRef survived serialization AS a ref + still resolves")
        (is (= (hasch/uuid v) (hasch/ref->uuid back-ref))
            "ref->uuid recovers the store key after a round-trip"))
      ;; transparency: a value holding the ref hashes identically to one holding the value
      (is (= (hasch/uuid {:k [{:elem ref} :plain]}) (hasch/uuid {:k [{:elem v} :plain]}))
          "break-out is invisible to the enclosing content-hash"))))

(deftest-async end-to-end-gc-follows-refs
  (testing "reachable-addresses follows a HashRef embedded in an element to its broken-out
            value (so GC keeps it), and does NOT reach an unreferenced broken-out value —
            the end-to-end-GC property that a plain PSS-only walk would miss."
    (let [{:keys [kv-store]} (<? (d/open (file-cfg) {} {:sync? sync?}))
          kv         kv-store
          referenced {:payload (vec (range 500))}
          orphan     {:payload (vec (range 500 1000))}
          ref        (<? (cas/break-out! kv referenced {:sync? sync?}))
          _          (<? (cas/break-out! kv orphan {:sync? sync?}))
          holder     {:level 0 :keys [{:elem ref} :plain]}
          holder-key (hasch/uuid holder)]
      (<? (kb/k-assoc kv holder-key holder nil {:sync? sync?}))
      (let [reach (<? (d/reachable-addresses kv holder-key {:sync? sync?}))]
        (is (contains? reach (hasch/uuid referenced))
            "a broken-out value referenced from a live element is REACHABLE (GC keeps it)")
        (is (not (contains? reach (hasch/uuid orphan)))
            "an unreferenced broken-out value is NOT reachable (GC sweeps it)")))))
