(ns yggdrasil.watcher
  "Shared polling watcher infrastructure.

  Provides a single ScheduledExecutorService (daemon threads) that runs
  poll tasks for all watched systems. Multiple watch! calls on the same
  system share one poll task; callbacks are dispatched on the poll thread.

  Usage:
    (def state (create-watcher-state))
    (start-polling! state poll-fn 1000)   ; poll every 1s
    (add-callback! state watch-id cb)
    (remove-callback! state watch-id)     ; stops polling when last removed"
  (:import [java.util.concurrent Executors ScheduledExecutorService
            ScheduledFuture TimeUnit ThreadFactory]))

(defonce ^:private ^ScheduledExecutorService executor
  (Executors/newScheduledThreadPool
   2
   (reify ThreadFactory
     (newThread [_ r]
       (doto (Thread. r "yggdrasil-watcher")
         (.setDaemon true))))))

(defn create-watcher-state
  "Create a mutable watcher state map (stored in adapter atoms).
   Contains callbacks, last-known state, and the scheduled future."
  []
  (atom {:callbacks {}         ; {watch-id -> callback-fn}
         :last-state nil       ; adapter-specific state snapshot
         :future nil}))        ; ScheduledFuture

(defn- run-poll-cycle!
  "Run one poll cycle: call poll-fn to get current state,
   compare to last-known, fire callbacks for detected events."
  [watcher-state poll-fn]
  (try
    (let [{:keys [callbacks last-state]} @watcher-state
          {:keys [state events]} (poll-fn last-state)]
      (when (seq events)
        (swap! watcher-state assoc :last-state state)
        (doseq [[_id cb] callbacks
                event events]
          (try
            (cb event)
            (catch Exception e
              (.printStackTrace e)))))
      ;; Update state even if no events (first poll)
      (when (nil? last-state)
        (swap! watcher-state assoc :last-state state)))
    (catch Exception e
      (.printStackTrace e))))

(defn start-polling!
  "Start polling with the given poll-fn and interval.
   poll-fn: (fn [last-state] {:state new-state, :events [event-maps]})
   Does nothing if already polling."
  [watcher-state poll-fn interval-ms]
  (when-not (:future @watcher-state)
    (let [task (fn [] (run-poll-cycle! watcher-state poll-fn))
          fut (.scheduleAtFixedRate executor
                                    ^Runnable task
                                    0 interval-ms TimeUnit/MILLISECONDS)]
      (swap! watcher-state assoc :future fut))))

(defn stop-polling!
  "Stop the poll task."
  [watcher-state]
  (when-let [^ScheduledFuture fut (:future @watcher-state)]
    (.cancel fut false)
    (swap! watcher-state assoc :future nil)))

(defn add-callback!
  "Register a callback. Returns watch-id."
  [watcher-state watch-id callback]
  (swap! watcher-state update :callbacks assoc watch-id callback)
  watch-id)

(defn remove-callback!
  "Remove a callback. Stops polling if no callbacks remain."
  [watcher-state watch-id]
  (swap! watcher-state update :callbacks dissoc watch-id)
  (when (empty? (:callbacks @watcher-state))
    (stop-polling! watcher-state)))
