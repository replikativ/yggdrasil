(ns yggdrasil.convergent.gset
  "Grow-only Set (G-Set) as a CONFLICT-FREE yggdrasil system — the smallest
   CRDT, and the spike that proves CRDTs are just yggdrasil's multi-head regime
   with a conflict-free 2-way join (no ancestor, no conflicts).

   Because `merge!` is set-union (commutative/associative/idempotent) and
   `conflicts` is always empty, a G-Set system flows through the EXISTING
   yggdrasil merge machinery (`merge!`/`conflicts`) and spindel's
   `merge-to-parent!` unchanged — no new merge code.

   Like datahike's adapter, branches share a backing `store` atom
   ({branch -> #{elements}}) so a forked value's branch is resolvable from the
   parent value; branch pointers themselves are value-semantic (the record
   carries only `:current`). This is the in-memory analogue of a shared konserve
   store + value-semantic conn.

   Spike placement: lives in yggdrasil for now; the full CRDT catalog
   (LWWR/ORMap/…) extracts to a cljc leaf lib (convergent-systems-plan L3)."
  (:require [clojure.set :as set]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]))

(declare ->GSetSystem)

(defrecord GSetSystem [id store current]
  ;; store : (atom {branch-keyword -> #{elements}}) shared across forks
  ;; current : keyword — this value's checked-out branch

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :gset)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :graphable false :overlayable false})

  p/Snapshotable
  (snapshot-id [_] (str (hash [current (get @store current #{})])))
  (parent-ids [_] #{})
  (as-of [_ _snap] (get @store current #{}))
  (as-of [this snap _opts] (p/as-of this snap))
  (snapshot-meta [_ _snap] {})
  (snapshot-meta [this snap _opts] (p/snapshot-meta this snap))

  p/Branchable
  (branches [_] (set (keys @store)))
  (branches [this _opts] (p/branches this))
  (current-branch [_] current)
  (branch! [this name]
    ;; create `name` from current state; current branch unchanged (value-semantic)
    (swap! store (fn [m] (assoc m name (get m current #{}))))
    this)
  (branch! [this name from]
    (swap! store (fn [m] (assoc m name (get m from #{}))))
    this)
  (branch! [this name from _opts] (p/branch! this name from))
  (delete-branch! [this name]
    (swap! store dissoc name)
    this)
  (delete-branch! [this name _opts] (p/delete-branch! this name))
  (checkout [this name] (assoc this :current name))
  (checkout [this name _opts] (assoc this :current name))

  p/Mergeable
  (merge! [this source] (p/merge! this source nil))
  (merge! [this source _opts]
    ;; THE JOIN: union the source branch's set into the current branch's set.
    ;; commutative + associative + idempotent ⇒ conflict-free convergence.
    (swap! store (fn [m] (update m current (fnil set/union #{}) (get m source #{}))))
    this)
  (conflicts [_ _a _b] [])              ; conflict-free: never any conflicts
  (conflicts [_ _a _b _opts] [])
  (diff [_ _a _b] {:added #{} :removed #{}})
  (diff [this a b _opts] (p/diff this a b))

  c/PConvergent
  ;; The NATIVE merge: symmetric, 2-way, no parent. Joins two PEER replicas
  ;; (separate stores) by per-branch set-union — pure (fresh store), so
  ;; (-join a b) ≡ (-join b a), idempotent, associative.
  (-join [this other]
    (->GSetSystem id
                  (atom (merge-with set/union @store @(:store other)))
                  current))
  (-conflict-free? [_] true))

(defn gset
  "Create a G-Set conflict-free system. `id` is its system-id; starts on branch
   `:main` (override with :branch) holding the optional `init` set."
  [id & {:keys [branch init] :or {branch :main init #{}}}]
  (->GSetSystem id (atom {branch (set init)}) branch))

(defn add
  "Add `x` to the current branch's set (the local op). Returns the system."
  [^GSetSystem g x]
  (swap! (:store g) update (:current g) (fnil conj #{}) x)
  g)

(defn elements
  "Read the current branch's set."
  [^GSetSystem g]
  (get @(:store g) (:current g) #{}))
