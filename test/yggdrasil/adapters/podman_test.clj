(ns yggdrasil.adapters.podman-test
  "Compliance tests for the Podman adapter.
   Tests are skipped when podman is not available."
  (:require [clojure.test :refer [deftest is testing]]
            [yggdrasil.adapters.podman :as pm]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p]
            [clojure.java.shell :refer [sh]])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; ============================================================
;; Podman availability check
;; ============================================================

(defn- podman-available?
  "Check if podman is usable."
  []
  (try
    (zero? (:exit (sh "podman" "version" "--format" "{{.Client.Version}}")))
    (catch Exception _ false)))

(defn- temp-dir []
  (str (Files/createTempDirectory "ygg-podman-test-"
                                  (make-array FileAttribute 0))))

(defn- make-fixture []
  {:create-system (fn []
                    (let [ws (temp-dir)]
                      (pm/init! ws {:base-image "ubuntu:24.04"})))
   :mutate (fn [sys]
             (pm/write-file! sys
                             (str "entries/mutate-" (System/nanoTime))
                             (str (System/nanoTime)))
             sys)
   :commit (fn [sys msg]
             (p/commit! sys msg))
   :close! (fn [sys]
             (pm/destroy! sys))
   :write-entry (fn [sys key value]
                  (pm/write-file! sys (str "entries/" key) value)
                  sys)
   :read-entry (fn [sys key]
                 (pm/read-file sys (str "entries/" key)))
   :count-entries (fn [sys]
                    (let [{:keys [system-id current-branch]} sys
                          cname (str "ygg-" system-id "-" current-branch)
                          result (sh "podman" "exec" cname
                                     "sh" "-c"
                                     "ls /entries 2>/dev/null | wc -l")]
                      (if (zero? (:exit result))
                        (Integer/parseInt (clojure.string/trim (:out result)))
                        0)))
   :delete-entry (fn [sys key]
                   (pm/delete-file! sys (str "entries/" key))
                   sys)
   :supports-concurrent? false})

;; ============================================================
;; Compliance tests (skipped when podman unavailable)
;; ============================================================

(deftest ^:podman full-compliance-suite
  (if-not (podman-available?)
    (println "SKIP: podman not available")
    (testing "podman adapter passes yggdrasil compliance suite"
      (compliance/run-compliance-tests (make-fixture)))))

(deftest ^:podman system-identity-tests
  (when (podman-available?)
    (let [fix (make-fixture)]
      (compliance/test-system-identity fix))))

(deftest ^:podman snapshotable-tests
  (when (podman-available?)
    (let [fix (make-fixture)]
      (compliance/test-snapshot-id-after-commit fix)
      (compliance/test-parent-ids-root-commit fix)
      (compliance/test-parent-ids-chain fix)
      (compliance/test-snapshot-meta fix))))

(deftest ^:podman branchable-tests
  (when (podman-available?)
    (let [fix (make-fixture)]
      (compliance/test-initial-branches fix)
      (compliance/test-create-branch fix)
      (compliance/test-checkout fix)
      (compliance/test-branch-isolation fix)
      (compliance/test-delete-branch fix))))

(deftest ^:podman graphable-tests
  (when (podman-available?)
    (let [fix (make-fixture)]
      (compliance/test-history fix)
      (compliance/test-history-limit fix)
      (compliance/test-ancestors fix)
      (compliance/test-ancestor-predicate fix))))

(deftest ^:podman data-consistency-tests
  (when (podman-available?)
    (let [fix (make-fixture)]
      (compliance/test-write-read-roundtrip fix)
      (compliance/test-count-after-writes fix)
      (compliance/test-multiple-entries-readable fix)
      (compliance/test-branch-data-isolation fix)
      (compliance/test-delete-entry-consistency fix)
      (compliance/test-overwrite-entry fix))))
