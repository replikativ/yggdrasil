(ns yggdrasil.convergent.lwwr
  "Last-Writer-Wins Register — a conflict-free yggdrasil system. value =
   `{:register v :hlc [physical logical]}`; join = keep the greater HLC, breaking
   exactly-equal HLCs deterministically (so all replicas converge on the same
   winner).

   The clock is a HYBRID LOGICAL CLOCK stored as a plain `[physical logical]`
   vector (trivially serializable; ordered by `compare`). `physical` is the
   wall-clock ms (read it for 'when this was written'); `logical` is a tie counter
   that ticks when the wall-clock can't advance. Two properties wall-clock-only LWW
   lacks:
   - **monotonic**: each write's HLC is strictly greater than the previous, even if
     the wall-clock steps BACKWARD (NTP) — so a single writer's register never
     silently loses a write to clock regression;
   - **causal**: `set-register` ticks from the register's CURRENT HLC (which, after
     a join, is the winner's), so a write that OBSERVED a peer ticks past it and
     wins even under clock skew. Genuinely-concurrent writes (equal/incomparable
     HLCs) still resolve by wall-clock then hash — the honest LWW behaviour.

   This is the convergent form of a server-authoritative signal (the checkout
   descriptor) AND the building block for presence. Generalizes
   replikativ.crdt.lwwr (plain wall-clock + pr-str tiebreak) to an HLC + a
   cross-platform hash tiebreak."
  (:require [yggdrasil.convergent.system :as sys]
            [yggdrasil.types :as t]
            [hasch.core :as hasch]
            [yggdrasil.fressian :as yf]))

(defn bump-hlc
  "Monotonic HLC tick from `[physical logical]` (or nil → epoch): advance physical
   to the wall-clock if it moved forward, else bump the logical counter — so the
   result is ALWAYS strictly greater than the input, even if the wall-clock stepped
   back. Reads the clock via `yggdrasil.types/now-ms` (injectable for replay)."
  [hlc]
  (let [[physical logical] (or hlc [0 0])
        now (t/now-ms)]
    (if (> now physical) [now 0] [physical (inc logical)])))

(defn lwwr-join
  "Least-upper-bound of two LWW registers: greater HLC wins (`compare` is
   lexicographic on `[physical logical]`); on an EXACTLY-equal HLC (genuinely
   concurrent), a deterministic, CROSS-PLATFORM-stable `hasch` tiebreak picks the
   same winner on every replica (NOT `pr-str`, whose map/set iteration order can
   differ JVM↔cljs and break convergence)."
  [a b]
  (cond
    (nil? a) b
    (nil? b) a
    :else (let [c (compare (:hlc a) (:hlc b))]
            (cond (pos? c) a
                  (neg? c) b
                  :else (if (pos? (compare (str (hasch/uuid b)) (str (hasch/uuid a)))) b a)))))

(defn lwwr
  "An LWW-Register conflict-free system. Optional `:init` seeds the register
   (stamped now); `:branch` defaults to :main."
  ([id] (lwwr id {}))
  ([id {:keys [branch init] :or {branch :main}}]
   (sys/conflict-free-system id :lwwr
                             {:branch branch
                              :init (when (some? init) {:register init :hlc [(t/now-ms) 0]})
                              :vjoin lwwr-join :bottom nil})))

(defn set-register
  "Write `v`, stamped with a monotonic HLC ticked from the register's CURRENT hlc
   (so observed/causally-later writes win, and a single writer never regresses).
   The local op."
  [l v]
  (let [reg {:register v :hlc (bump-hlc (:hlc (sys/cur l)))}]
    (sys/record-delta (sys/put! l reg) reg)))

(defn value
  "Read the current register value (nil if unset)."
  [l]
  (:register (sys/cur l)))

(defn timestamp
  "The wall-clock millis of the current register's write (the HLC's physical part),
   or nil if unset."
  [l]
  (first (:hlc (sys/cur l))))

;; Register the LWW-Register with the system value codec (JVM). An IN-MEMORY
;; ConflictFreeSystem: the whole {branch -> {:register :hlc}} store is plain data, so
;; it projects verbatim — no PSS roots, no storage. Only the join fn isn't a value;
;; it's re-injected (lwwr-join) on read.
(yf/register-system!
 :lwwr #?(:clj (class (lwwr "_probe")) :cljs (type (lwwr "_probe")))
 (fn [{:keys [id store current config]}]
   {:id id :store store :current current :config config})
 (fn [blob _storage _opts]
   (sys/->ConflictFreeSystem (:id blob) :lwwr (:store blob) (:current blob)
                             lwwr-join nil (:config blob))))
