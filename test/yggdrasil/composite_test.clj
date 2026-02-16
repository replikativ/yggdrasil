(ns yggdrasil.composite-test
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.composite :as composite]
            [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]))

;; ============================================================
;; Mock system — value-semantic, branchable, committable
;; ============================================================

(defrecord MockSystem [id branch-name snapshots-atom branches-atom]
  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :mock)
  (capabilities [_]
    (t/->Capabilities true true true true false false true false true))

  p/Snapshotable
  (snapshot-id [_] (last @snapshots-atom))
  (parent-ids [_]
    (let [snaps @snapshots-atom]
      (if (> (count snaps) 1)
        #{(nth snaps (- (count snaps) 2))}
        #{})))
  (as-of [_ snap-id] snap-id)
  (as-of [_ snap-id _] snap-id)
  (snapshot-meta [_ snap-id] {:snapshot-id snap-id})
  (snapshot-meta [_ snap-id _] {:snapshot-id snap-id})

  p/Branchable
  (branches [_] @branches-atom)
  (branches [_ _] @branches-atom)
  (current-branch [_] (keyword branch-name))
  (branch! [this name]
    (let [branch-str (clojure.core/name name)]
      (swap! branches-atom conj (keyword name))
      (assoc this :snapshots-atom
             (atom (vec @snapshots-atom)))))
  (branch! [this name _from] (p/branch! this name))
  (branch! [this name _from _opts] (p/branch! this name))
  (delete-branch! [this name]
    (swap! branches-atom disj (keyword name))
    this)
  (delete-branch! [this name _] (p/delete-branch! this name))
  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _]
    (let [branch-str (clojure.core/name name)]
      (when-not (contains? @branches-atom (keyword name))
        (throw (ex-info (str "Branch not found: " name)
                        {:branch name})))
      (assoc this :branch-name branch-str)))

  p/Committable
  (commit! [this] (p/commit! this nil nil))
  (commit! [this message] (p/commit! this message nil))
  (commit! [this message _opts]
    (let [snap-id (str (java.util.UUID/randomUUID))]
      (swap! snapshots-atom conj snap-id)
      this))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ _opts] (vec (reverse @snapshots-atom)))
  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _]
    (let [snaps @snapshots-atom
          idx (.indexOf snaps (str snap-id))]
      (if (pos? idx)
        (vec (take idx snaps))
        [])))
  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [this a b _]
    (contains? (set (p/ancestors this b)) (str a)))
  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [this a b _]
    (let [anc-a (set (p/ancestors this a))
          anc-b (set (p/ancestors this b))]
      (first (filter anc-a (reverse (p/ancestors this b))))))
  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _]
    {:nodes {} :branches {} :roots #{}})
  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [_ snap-id _] {:snapshot-id snap-id})

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this _source _opts] this)
  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ _ _ _] [])
  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _] {:from a :to b})

  p/GarbageCollectable
  (gc-roots [_]
    #{(last @snapshots-atom)})
  (gc-sweep! [this ids] (p/gc-sweep! this ids nil))
  (gc-sweep! [this _ _] this))

(defn- make-mock [id initial-snap]
  (->MockSystem id "main" (atom [initial-snap]) (atom #{:main})))

;; ============================================================
;; Tests
;; ============================================================

(deftest test-pullback-creation
  (testing "pullback requires all systems on the same branch"
    (let [a (make-mock "sys-a" "snap-a0")
          b (make-mock "sys-b" "snap-b0")]
      (is (some? (composite/pullback [a b] :name "test-composite")))))

  (testing "pullback rejects mismatched branches"
    (let [a (make-mock "sys-a" "snap-a0")
          b (assoc (make-mock "sys-b" "snap-b0") :branch-name "develop")]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"fiber condition"
                            (composite/pullback [a b]))))))

(deftest test-system-identity
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c (composite/pullback [a b] :name "my-composite")]
    (testing "system-id"
      (is (= "my-composite" (p/system-id c))))
    (testing "system-type"
      (is (= :composite (p/system-type c))))
    (testing "auto-generated name uses × separator"
      (let [auto (composite/pullback [a b])]
        (is (= "pullback:sys-a×sys-b" (p/system-id auto)))))))

(deftest test-snapshot-id-deterministic
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c1 (composite/pullback [a b])
        c2 (composite/pullback [a b])]
    (testing "same sub-system states produce same composite ID"
      (is (= (p/snapshot-id c1) (p/snapshot-id c2))))))

(deftest test-branches-intersection
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c (composite/pullback [a b])]
    (testing "initial branches"
      (is (= #{:main} (p/branches c))))
    (testing "branch creates on both sub-systems"
      (let [c2 (p/branch! c :feature)]
        (is (= #{:main :feature} (p/branches c2)))))
    (testing "current-branch preserved"
      (is (= :main (p/current-branch c))))))

(deftest test-checkout
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c (composite/pullback [a b])
        c2 (-> c (p/branch! :feature) (p/checkout :feature))]
    (testing "checkout switches current branch"
      (is (= :feature (p/current-branch c2))))
    (testing "both sub-systems switched"
      (doseq [[_ sys] (:systems c2)]
        (is (= :feature (p/current-branch sys)))))))

(deftest test-commit-updates-snapshot
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c (composite/pullback [a b])
        snap-before (p/snapshot-id c)
        c2 (p/commit! c "test commit")
        snap-after (p/snapshot-id c2)]
    (testing "commit changes composite snapshot-id"
      (is (not= snap-before snap-after)))
    (testing "parent-ids link to previous"
      (is (= #{snap-before} (p/parent-ids c2))))
    (testing "snapshot-meta records message"
      (let [meta (p/snapshot-meta c2 snap-after)]
        (is (= "test commit" (:message meta)))
        (is (some? (:sub-snapshots meta)))))))

(deftest test-history-chain
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c (composite/pullback [a b])
        c1 (p/commit! c "first")
        c2 (p/commit! c1 "second")
        c3 (p/commit! c2 "third")
        hist (p/history c3)]
    (testing "history walks composite parent chain"
      (is (= 4 (count hist)))  ;; initial + 3 commits
      (is (= (p/snapshot-id c3) (first hist))))))

(deftest test-commit-graph
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c (composite/pullback [a b])
        c2 (p/commit! c "first")
        graph (p/commit-graph c2)]
    (testing "commit-graph has nodes and branches"
      (is (= 2 (count (:nodes graph))))
      (is (contains? (:branches graph) :main)))))

(deftest test-delete-branch
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c (-> (composite/pullback [a b])
              (p/branch! :feature))]
    (testing "delete-branch removes from all sub-systems"
      (let [c2 (p/delete-branch! c :feature)]
        (is (= #{:main} (p/branches c2)))))))

(deftest test-gc-roots-union
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c (composite/pullback [a b])]
    (testing "gc-roots is union of sub-system roots"
      (is (= #{"snap-a0" "snap-b0"} (p/gc-roots c))))))

(deftest test-as-of-with-known-snapshot
  (let [a (make-mock "sys-a" "snap-a0")
        b (make-mock "sys-b" "snap-b0")
        c (composite/pullback [a b])
        snap (p/snapshot-id c)
        view (p/as-of c snap)]
    (testing "as-of returns per-system views for known composite ID"
      (is (map? view))
      (is (= 2 (count view)))
      (is (contains? view "sys-a"))
      (is (contains? view "sys-b")))))

(deftest test-monoid-associativity
  (testing "pullback is monoidal: pullback([a, b, c]) works"
    (let [a (make-mock "sys-a" "snap-a0")
          b (make-mock "sys-b" "snap-b0")
          c (make-mock "sys-c" "snap-c0")
          flat (composite/pullback [a b c])
          ;; nested would require CompositeSystem to also implement the mock
          ;; protocols, but the flat 3-way pullback validates the monoidal property
          ]
      (is (= 3 (count (:systems flat))))
      (is (= #{:main} (p/branches flat))))))

;; ============================================================
;; composite (lenient constructor) tests
;; ============================================================

(deftest test-composite-allows-different-branches
  (testing "composite does not enforce same-branch constraint"
    (let [a (make-mock "sys-a" "snap-a0")
          b (assoc (make-mock "sys-b" "snap-b0") :branch-name "db")]
      ;; pullback would reject this
      (is (thrown? clojure.lang.ExceptionInfo
                   (composite/pullback [a b])))
      ;; composite accepts it with explicit :branch
      (let [c (composite/composite [a b] :branch :main)]
        (is (= :main (p/current-branch c)))
        (is (= "composite:sys-a+sys-b" (p/system-id c)))))))

(deftest test-composite-default-branch
  (testing "composite defaults to :main branch"
    (let [a (make-mock "sys-a" "snap-a0")
          b (make-mock "sys-b" "snap-b0")
          c (composite/composite [a b])]
      (is (= :main (p/current-branch c))))))

(deftest test-get-subsystem
  (testing "get-subsystem retrieves individual sub-systems"
    (let [a (make-mock "sys-a" "snap-a0")
          b (make-mock "sys-b" "snap-b0")
          c (composite/pullback [a b])]
      (is (= "sys-a" (p/system-id (composite/get-subsystem c "sys-a"))))
      (is (= "sys-b" (p/system-id (composite/get-subsystem c "sys-b"))))
      (is (nil? (composite/get-subsystem c "nonexistent"))))))
