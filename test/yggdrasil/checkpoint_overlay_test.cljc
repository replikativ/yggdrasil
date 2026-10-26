(ns yggdrasil.checkpoint-overlay-test
  (:require #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is]])
            [yggdrasil.protocols :as p]))

(def checkpoint
  {:version 1 :kind :datahike :commit #uuid "d55debd7-26fe-475b-b122-fdf19a68a703"
   :source {:store #uuid "36764c23-109c-4b30-afd1-fd641969809a"
            :repository #uuid "225883a0-cfad-441a-8cb4-34e33bd01253"}})

(defn- system
  ([advertised calls result] (system advertised calls result #{:sync :async}))
  ([advertised calls result modes]
   (reify
     p/SystemIdentity
     (system-id [_] "checkpoint-fixture")
     (system-type [_] :fixture)
     (capabilities [_] {:overlayable true :checkpoint-overlay advertised :checkpoint-overlay-modes modes})
     p/Overlayable
     (overlay [_ opts] (swap! calls conj opts) result)
     (advance! [this] this)
     (advance! [this _] this)
     (peek-parent [_] nil)
     (peek-parent [_ _] nil)
     (base-ref [_] nil)
     (overlay-writes [_] nil)
     (merge-down! [_] nil)
     (merge-down! [_ _] nil)
     (discard! [_] nil)
     (discard! [_ _] nil))))

(deftest checkpoint-origin-is-closed-inert-identity
  (is (p/checkpoint-origin? checkpoint))
  (doseq [bad [nil {} (assoc checkpoint :version 2) (assoc checkpoint :kind :git)
               (assoc checkpoint :commit (str (:commit checkpoint)))
               (assoc checkpoint :conn :native) (assoc-in checkpoint [:source :path] "/private")
               (dissoc checkpoint :source) (assoc-in checkpoint [:source :repository] nil)]]
    (is (not (p/checkpoint-origin? bad)))))

(deftest checkpoint-requests-require-positive-capability-and-unambiguous-origin
  (let [calls (atom [])]
    (doseq [advertised [nil false :yes 1]]
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (p/overlay-at-checkpoint (system advertised calls :bad) checkpoint))))
    (let [s (system true calls :good)]
      (doseq [opts [[] {:sync? nil} {:sync? :yes} {:unknown true} {:mode :following} {:mode nil} {:snapshot "logical"} {:checkpoint checkpoint}]]
        (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error) (p/overlay-at-checkpoint s checkpoint opts))))
      (is (empty? @calls))
      (is (= :good (p/overlay-at-checkpoint s checkpoint)))
      (is (= [{:checkpoint checkpoint :mode :frozen}] @calls))
      (reset! calls [])
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (p/overlay-at s "logical" {:checkpoint checkpoint})))
      (is (empty? @calls)))))

(deftest checkpoint-constructor-preserves-the-adapters-cps-value
  (let [calls (atom [])
        cps (fn [_ _] (throw (ex-info "Must not execute during construction" {})))]
    (is (identical? cps (p/overlay-at-checkpoint (system true calls cps) checkpoint {:sync? false})))
    (is (= [{:checkpoint checkpoint :mode :frozen :sync? false}] @calls))))

(deftest execution-capability-is-preflighted-without-adapter-work
  (let [calls (atom [])]
    (doseq [mode [:sync :async]]
      (let [s (system true calls :ok #{mode})
            supported (= mode :sync) unsupported (not supported)]
        (is (p/supports-checkpoint-overlay? s {:sync? supported}))
        (is (not (p/supports-checkpoint-overlay? s {:sync? unsupported})))
        (is (= (= mode #?(:clj :sync :cljs :async)) (p/supports-checkpoint-overlay? s)))
        (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                     (p/overlay-at-checkpoint s checkpoint {:sync? unsupported})))
        (is (empty? @calls))
        (is (= :ok (p/overlay-at-checkpoint s checkpoint {:sync? supported})))
        (is (= [{:checkpoint checkpoint :mode :frozen :sync? supported}] @calls))
        (reset! calls [])))))

(defn- validating-system [calls result modes]
  (reify
    p/SystemIdentity
    (system-id [_] "validating-checkpoint-fixture")
    (system-type [_] :fixture)
    (capabilities [_] {:checkpoint-overlay true :checkpoint-overlay-modes modes})
    p/CheckpointOriginValidator
    (-validate-checkpoint-origin [_ descriptor opts]
      (swap! calls conj [descriptor opts]) result)
    p/Overlayable
    (overlay [_ _] (throw (ex-info "Validation must not allocate" {})))
    (advance! [this] this)
    (advance! [this _] this)
    (peek-parent [_] nil)
    (peek-parent [_ _] nil)
    (base-ref [_] nil)
    (overlay-writes [_] nil)
    (merge-down! [_] nil)
    (merge-down! [_ _] nil)
    (discard! [_] nil)
    (discard! [_ _] nil)))

(deftest checkpoint-validation-requires-the-real-optional-protocol
  (let [calls (atom []) forged (system true calls :allocated)]
    (is (p/supports-checkpoint-overlay? forged))
    (is (not (p/supports-checkpoint-validation? forged)))
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                 (p/validate-checkpoint-origin! forged checkpoint)))
    (is (empty? @calls))))

(deftest checkpoint-validation-preflights-closed-shape-and-execution
  (let [calls (atom []) s (validating-system calls checkpoint #{:sync :async})]
    (is (p/supports-checkpoint-validation? s))
    (is (empty? @calls) "Support predicate must never invoke validation")
    (doseq [bad [nil {} (assoc checkpoint :native :handle)
                 (assoc checkpoint :commit "logical")]]
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (p/validate-checkpoint-origin! s bad))))
    (doseq [opts [[] {:sync? nil} {:mode :following} {:snapshot "logical"} {:unknown true}]]
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (p/validate-checkpoint-origin! s checkpoint opts))))
    (is (empty? @calls))
    (is (identical? checkpoint (p/validate-checkpoint-origin! s checkpoint)))
    (is (= [[checkpoint {}]] @calls))
    (reset! calls [])
    (let [unsupported (validating-system calls checkpoint #{:async})]
      (is (not (p/supports-checkpoint-validation? unsupported {:sync? true})))
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (p/validate-checkpoint-origin! unsupported checkpoint {:sync? true})))
      (is (empty? @calls)))))

(deftest checkpoint-validation-preserves-cps-without-running-it
  (let [calls (atom []) cps (fn [_ _] (throw (ex-info "Not during preflight dispatch" {})))
        s (validating-system calls cps #{:sync :async})]
    (is (identical? cps (p/validate-checkpoint-origin! s checkpoint {:sync? false})))
    (is (= [[checkpoint {:sync? false}]] @calls))))
