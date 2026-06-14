(ns yggdrasil.convergent.gset
  "Grow-only Set (G-Set) — the smallest CRDT, as a conflict-free yggdrasil
   system. value = a set; join = `set/union` (commutative/associative/idempotent).
   The system machinery (branch/checkout/merge/-join/fork-sharing) is the generic
   `yggdrasil.convergent.system`; this ns is just the value-join + helpers."
  (:require [clojure.set :as set]
            [yggdrasil.convergent.system :as sys]))

(defn gset
  "A G-Set conflict-free system, id = system-id, starting on `:branch` (:main)
   holding the optional `:init` set."
  [id & {:keys [branch init] :or {branch :main}}]
  (sys/conflict-free-system id :gset
                            :branch branch :init (set init)
                            :vjoin set/union :bottom #{}))

(defn add "Add `x` to the current branch's set (local op)." [g x]
  (sys/record-delta (sys/upd! g conj x) #{x}))

(defn elements "Read the current branch's set." [g]
  (sys/cur g))
