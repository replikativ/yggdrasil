(ns yggdrasil.convergent.lwwr
  "Last-Writer-Wins Register — a conflict-free yggdrasil system. value =
   `{:register v :timestamp ms}`; join = keep the greater timestamp, breaking
   ties deterministically (so all replicas converge on the same winner).

   This is the honest convergent form of a server-authoritative signal (the
   checkout descriptor) AND the building block for presence: stamp every write
   with a clock, last-writer-wins. Ported from replikativ.crdt.lwwr."
  (:require [yggdrasil.convergent.system :as sys]
            [yggdrasil.types :as t]))

(defn lwwr-join
  "Least-upper-bound of two LWW registers: greater timestamp wins; on a tie,
   a deterministic `pr-str` comparison picks the same winner on every replica."
  [a b]
  (cond
    (nil? a) b
    (nil? b) a
    :else (let [ta (:timestamp a 0) tb (:timestamp b 0)]
            (cond (> tb ta) b
                  (< tb ta) a
                  :else (if (pos? (compare (pr-str b) (pr-str a))) b a)))))

(defn lwwr
  "An LWW-Register conflict-free system. Optional `:init` seeds the register
   (stamped now); `:branch` defaults to :main."
  [id & {:keys [branch init] :or {branch :main}}]
  (sys/conflict-free-system id :lwwr
                            :branch branch
                            :init (when (some? init) {:register init :timestamp (t/now-ms)})
                            :vjoin lwwr-join :bottom nil))

(defn set-register
  "Write `v` (stamped now) — the local op."
  [l v]
  (sys/put! l {:register v :timestamp (t/now-ms)}))

(defn value
  "Read the current register value (nil if unset)."
  [l]
  (:register (sys/cur l)))
