(ns yggdrasil.adapters.git-test
  "Compliance tests for the Git adapter (worktree-based)."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.adapters.git :as git]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-dir []
  (str (Files/createTempDirectory "yggdrasil-git-test-"
                                  (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(defn- wt-path
  "Get the worktree path for the current branch of a GitSystem."
  [sys]
  (let [branch @(:current-branch-atom sys)]
    (if (= branch "main")
      (:repo-path sys)
      (str (:worktrees-dir sys) "/" branch))))

(defn- git-cmd [wt-dir & args]
  (let [result (apply clojure.java.shell/sh "git" "-C" wt-dir args)]
    (when-not (zero? (:exit result))
      (throw (ex-info "git failed" {:err (:err result) :args (vec args)})))
    (clojure.string/trim (:out result))))

(defn- make-fixture []
  {:create-system (fn []
                    (let [path (temp-dir)]
                      (git/init! path {:system-name "test-git"})))
   :mutate (fn [sys]
             (let [wt (wt-path sys)
                   f (str wt "/entries/mutate-" (System/nanoTime))]
               (spit f (str (System/nanoTime)))
               sys))
   :commit (fn [sys msg]
             (let [wt (wt-path sys)]
               (git-cmd wt "add" "-A")
               (git-cmd wt "commit" "-m" (or msg "commit") "--allow-empty")
               sys))
   :close! (fn [sys]
             (delete-dir-recursive (:repo-path sys))
             (delete-dir-recursive (:worktrees-dir sys)))
   :write-entry (fn [sys key value]
                  (let [wt (wt-path sys)
                        f (str wt "/entries/" key)]
                    (spit f value)
                    sys))
   :read-entry (fn [sys key]
                 (let [wt (wt-path sys)
                       f (java.io.File. (str wt "/entries/" key))]
                   (when (.exists f)
                     (slurp f))))
   :count-entries (fn [sys]
                    (let [wt (wt-path sys)
                          dir (java.io.File. (str wt "/entries/"))]
                      (if (.exists dir)
                        (count (filter #(.isFile %) (.listFiles dir)))
                        0)))
   :delete-entry (fn [sys key]
                   (let [wt (wt-path sys)
                         f (java.io.File. (str wt "/entries/" key))]
                     (when (.exists f)
                       (.delete f))
                     sys))
   ;; Git supports concurrent branch operations via worktrees,
   ;; but same-branch concurrent commits still need serialization
   :supports-concurrent? false})

;; ============================================================
;; Run all compliance tests
;; ============================================================

(deftest ^:compliance full-compliance-suite
  (testing "git adapter passes yggdrasil compliance suite"
    (compliance/run-compliance-tests (make-fixture))))

;; ============================================================
;; Individual test groups (for targeted debugging)
;; ============================================================

(deftest system-identity-tests
  (let [fix (make-fixture)]
    (compliance/test-system-identity fix)))

(deftest snapshotable-tests
  (let [fix (make-fixture)]
    (compliance/test-snapshot-id-after-commit fix)
    (compliance/test-parent-ids-root-commit fix)
    (compliance/test-parent-ids-chain fix)
    (compliance/test-snapshot-meta fix)
    (compliance/test-as-of fix)))

(deftest branchable-tests
  (let [fix (make-fixture)]
    (compliance/test-initial-branches fix)
    (compliance/test-create-branch fix)
    (compliance/test-checkout fix)
    (compliance/test-branch-isolation fix)
    (compliance/test-delete-branch fix)))

(deftest graphable-tests
  (let [fix (make-fixture)]
    (compliance/test-history fix)
    (compliance/test-history-limit fix)
    (compliance/test-ancestors fix)
    (compliance/test-ancestor-predicate fix)
    (compliance/test-common-ancestor fix)
    (compliance/test-commit-graph fix)
    (compliance/test-commit-info fix)))

(deftest mergeable-tests
  (let [fix (make-fixture)]
    (compliance/test-merge fix)
    (compliance/test-merge-parent-ids fix)
    (compliance/test-conflicts-empty-for-compatible fix)))

(deftest data-consistency-tests
  (let [fix (make-fixture)]
    (compliance/test-write-read-roundtrip fix)
    (compliance/test-count-after-writes fix)
    (compliance/test-multiple-entries-readable fix)
    (compliance/test-branch-data-isolation fix)
    (compliance/test-merge-data-visibility fix)
    (compliance/test-as-of-data-consistency fix)
    (compliance/test-delete-entry-consistency fix)
    (compliance/test-overwrite-entry fix)))

(deftest watcher-test
  (testing "polling watcher detects commits"
    (let [path (temp-dir)
          sys (git/init! path)
          events (atom [])
          watch-id (p/watch! sys (fn [e] (swap! events conj e))
                             {:poll-interval-ms 100})]
      (try
        ;; Make a commit
        (spit (str path "/entries/test") "hello")
        (git-cmd path "add" "-A")
        (git-cmd path "commit" "-m" "watched commit")
        ;; Wait for poll to detect
        (Thread/sleep 300)
        (is (pos? (count @events))
            "Should have detected at least one event")
        (is (some #(= :commit (:type %)) @events)
            "Should have detected a commit event")
        (finally
          (p/unwatch! sys watch-id)
          (delete-dir-recursive path)
          (delete-dir-recursive (str path "-worktrees")))))))
