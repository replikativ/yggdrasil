(ns yggdrasil.convergent.system
  "Generic CONFLICT-FREE yggdrasil system, parameterized by a value-level join.

   Every CRDT in the catalog (G-Set, LWW-Register, OR-Map, …) is the same system
   machinery — SystemIdentity / Snapshotable / Branchable / Mergeable (conflicts
   ⇒ []) / PConvergent (`-join`) over a shared `{branch -> value}` store — and
   differs ONLY in `vjoin` (the value-level least-upper-bound) and `bottom` (the
   empty value). So a new CRDT is a few lines: a constructor + value read/write
   helpers; the convergence + branch + fork-sharing semantics come for free.

   `vjoin` MUST be commutative, associative, idempotent (that's what makes the
   system conflict-free). The branch backing is a shared atom, the in-memory
   analogue of datahike's shared konserve store + value-semantic conn."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]))

(declare ->ConflictFreeSystem)

(defrecord ConflictFreeSystem [id stype store current vjoin bottom]
  ;; store : (atom {branch -> value}) shared across forks/replicas
  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] stype)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :graphable false :overlayable false})

  p/Snapshotable
  (snapshot-id [_] (str (hash [current (get @store current)])))
  (parent-ids [_] #{})
  (as-of [_ _] (get @store current)) (as-of [_ _ _] (get @store current))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  p/Branchable
  (branches [_] (set (keys @store)))
  (branches [_ _] (set (keys @store)))
  (current-branch [_] current)
  (branch! [this name] (swap! store assoc name (get @store current bottom)) this)
  (branch! [this name from] (swap! store assoc name (get @store from bottom)) this)
  (branch! [this name from _] (swap! store assoc name (get @store from bottom)) this)
  (delete-branch! [this name] (swap! store dissoc name) this)
  (delete-branch! [this name _] (swap! store dissoc name) this)
  (checkout [this name] (assoc this :current name))
  (checkout [this name _] (assoc this :current name))

  p/Mergeable
  ;; branch-merge IS the value join (conflict-free) — same join the symmetric
  ;; -join uses, just source = another branch in the shared store.
  (merge! [this source] (p/merge! this source nil))
  (merge! [this source _]
    (swap! store update current (fn [v] (vjoin (or v bottom) (get @store source bottom))))
    this)
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {})

  c/PConvergent
  ;; symmetric peer join: per-branch value-join of two replicas' stores (pure).
  (-join [this other]
    (->ConflictFreeSystem id stype
                          (atom (merge-with vjoin @store @(:store other)))
                          current vjoin bottom))
  (-conflict-free? [_] true))

(defn conflict-free-system
  "Construct a conflict-free system. `vjoin`/`bottom` define the CRDT."
  [id stype & {:keys [branch init vjoin bottom] :or {branch :main}}]
  (->ConflictFreeSystem id stype (atom {branch (if (nil? init) bottom init)})
                        branch vjoin bottom))

;; value-level accessors (operate on the current branch's value)
(defn cur "Current branch's value." [s] (get @(:store s) (:current s) (:bottom s)))
(defn put! "Set the current branch's value. Returns the system." [s v]
  (swap! (:store s) assoc (:current s) v) s)
(defn upd! "Update the current branch's value with f. Returns the system." [s f & args]
  (apply swap! (:store s) update (:current s) (fnil f (:bottom s)) args) s)
