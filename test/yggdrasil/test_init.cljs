(ns yggdrasil.test-init
  "Shadow :node-test `:init-fn` — quiet konserve/replikativ trove logging before
   the suite runs. konserve logs every store op at `:trace`; the durable CRDT
   tests perform tens of thousands of ops, so trace logging floods stdout and
   dominates (often appears to hang) the node run. Keep `:warn` and above so
   genuine problems still surface."
  (:require [taoensso.trove :as trove]
            [taoensso.trove.console :as console]))

(def ^:private rank
  {:trace 0 :debug 1 :info 2 :warn 3 :error 4 :fatal 5 :report 6})

(defn init! []
  (let [base (console/get-log-fn)]
    (trove/set-log-fn!
     (fn [ns coords level id lazy_]
       (when (>= (rank level 6) (rank :warn))
         (base ns coords level id lazy_))))))
