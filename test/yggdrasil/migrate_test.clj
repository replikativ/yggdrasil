(ns yggdrasil.migrate-test
  "0.2 → 0.3 registry store migration. Hand-builds a 0.2-shaped store (a single PSS
   leaf of entry-maps under `:registry/roots {:tsbs <addr>}` — the unchanged node
   format) and asserts the converter rebuilds it as a readable 0.3 2P-Set registry."
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.test-async :refer [file-cfg]]
            [yggdrasil.kbridge :as kb]
            [yggdrasil.storage :as store]
            [yggdrasil.registry :as registry]
            [yggdrasil.convergent.gset :as g]
            [yggdrasil.convergent.durable :as d]
            [org.replikativ.persistent-sorted-set.fressian :as pss-fress]
            [yggdrasil.migrate :as migrate]))

(def ^:private sync {:sync? true})

(defn- entry-map [sid sys]
  {:snapshot-id sid :system-id sys :branch-name :main
   :hlc {:physical 1000 :logical 0} :content-hash sid :parent-ids #{} :metadata {}})

(deftest registry-0.2->0.3
  (testing "a 0.2 :registry/roots {:tsbs leaf} store migrates to a 0.3 2P-Set"
    (let [sc       (file-cfg)
          kv       (store/open-store sc sync)
          ;; a single 0.2-format leaf node (level 0, no :addresses), keys = entry-maps
          leaf     {:level 0 :keys [(entry-map "s1" "sysA") (entry-map "s2" "sysB")]
                    :addresses nil}
          leaf-addr (random-uuid)]
      (kb/k-assoc kv leaf-addr leaf sync)
      (kb/k-assoc kv :registry/roots {:tsbs leaf-addr} sync)
      (testing "migrate rebuilds the entries into a 0.3 registry on the same store"
        (let [reg     (migrate/migrate-registry-0.2->0.3! sc)
              systems (set (map :system-id (registry/all-entries reg)))]
          (is (= #{"sysA" "sysB"} systems) "both entries carried over")
          (is (= 2 (registry/entry-count reg)))
          (is (some? (kb/k-get (:kv-store reg) :crdt/roots sync)) "0.3 :crdt/roots written")))
      (testing "reopening at 0.3 (fresh registry) sees the migrated entries durably"
        (let [reg2 (registry/create-registry {:store-config sc})]
          (is (= #{"sysA" "sysB"} (set (map :system-id (registry/all-entries reg2))))))))))

(deftest migrate-is-idempotent-on-0.3-store
  (testing "a store with no :registry/roots (already 0.3 / fresh) migrates to a no-op"
    (let [sc  (file-cfg)
          reg (migrate/migrate-registry-0.2->0.3! sc)]
      (is (= 0 (registry/entry-count reg)) "nothing to migrate → empty registry"))))

(defn- downgrade-nodes!
  "Rewrite every reachable node in `sc` from the canonical tagged OBJECT form back
   to the pre-canonical untagged plain-map form ({:level :keys :addresses}) — to
   synthesize a pre-canonical store from a real one. Returns the address set."
  [sc]
  (let [{:keys [kv-store]} (d/open sc sync)
        roots (d/load-roots kv-store sync)
        addrs (into #{} (mapcat #(d/reachable-addresses kv-store % sync) (vals roots)))]
    (doseq [addr addrs]
      (let [obj (kb/k-get kv-store addr sync)]
        (kb/k-assoc kv-store addr
                    (select-keys (pss-fress/node->map obj) [:level :keys :addresses])
                    sync)))
    addrs))

(deftest pre-canonical-node-format-migration
  (testing "a store of untagged plain-map nodes migrates to the canonical tagged
            codec in place, and the CRDT reads back identically"
    (let [sc    (file-cfg)
          elems (set (map #(keyword (str "e" %)) (range 600)))  ; > branching-factor → a real branch
          g     (-> (reduce g/conj (g/gset "t" {:store-config sc :sync? true}) elems)
                    (g/flush!))]
      (is (= elems (g/elements g)) "canonical G-Set built")
      ;; synthesize a pre-canonical store: every node back to an untagged plain map
      (let [addrs (downgrade-nodes! sc)]
        (is (pos? (count addrs)) "tree has multiple nodes")
        (testing "a downgraded store has at least one branch (multi-node tree)"
          (let [{:keys [kv-store]} (d/open sc sync)]
            (is (some #(:addresses (kb/k-get kv-store % sync)) addrs)
                "at least one branch node (plain map with :addresses)")))
        ;; migrate the plain-map nodes back to canonical tagged objects, in place
        (let [n (migrate/migrate-store-nodes! sc)]
          (is (= (count addrs) n) "every node rewritten to canonical form"))
        (testing "the G-Set reopens + reads back every element after migration"
          (let [g2 (g/gset "t" {:store-config sc :sync? true})]
            (is (= elems (g/elements g2)))))
        (testing "migrate-store-nodes! is idempotent (canonical store → 0 rewrites)"
          (is (= 0 (migrate/migrate-store-nodes! sc))))))))
