(ns yggdrasil.migrate-test
  "0.2 → 0.3 registry store migration. Hand-builds a 0.2-shaped store (a single PSS
   leaf of entry-maps under `:registry/roots {:tsbs <addr>}` — the unchanged node
   format) and asserts the converter rebuilds it as a readable 0.3 2P-Set registry."
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.test-async :refer [file-cfg]]
            [yggdrasil.kbridge :as kb]
            [yggdrasil.storage :as store]
            [yggdrasil.registry :as registry]
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

(deftest registry-0.2->0.3-multi-node-tree
  (testing "a released (origin/main) registry whose :tsbs index is a BRANCH over
            multiple leaves migrates — proves branch traversal + the untagged
            plain-map nodes read straight through the canonical serializer"
    (let [sc        (file-cfg)
          kv        (store/open-store sc sync)
          ;; origin/main node format: untagged plain maps; leaf :keys are entry->map maps,
          ;; a branch carries :addresses. Build a 1-branch / 2-leaf tree by hand.
          leaf-a    {:level 0 :keys [(entry-map "s1" "sysA") (entry-map "s2" "sysB")] :addresses nil}
          leaf-b    {:level 0 :keys [(entry-map "s3" "sysC") (entry-map "s4" "sysD")] :addresses nil}
          a-addr    (random-uuid)
          b-addr    (random-uuid)
          branch    {:level 1 :keys [(entry-map "s3" "sysC")] :addresses [a-addr b-addr]}
          br-addr   (random-uuid)]
      (kb/k-assoc kv a-addr leaf-a sync)
      (kb/k-assoc kv b-addr leaf-b sync)
      (kb/k-assoc kv br-addr branch sync)
      (kb/k-assoc kv :registry/roots {:tsbs br-addr} sync)
      (let [reg     (migrate/migrate-registry-0.2->0.3! sc)
            systems (set (map :system-id (registry/all-entries reg)))]
        (is (= #{"sysA" "sysB" "sysC" "sysD"} systems) "all leaves' entries migrated through the branch")
        (is (some? (kb/k-get (:kv-store reg) :crdt/roots sync)) "0.3 :crdt/roots written")))))
