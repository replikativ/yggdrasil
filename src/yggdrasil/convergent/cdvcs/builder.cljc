(ns ^:no-doc yggdrasil.convergent.cdvcs.builder
  "The PRODUCTION commit builders the durable CDVCS wrapper (`yggdrasil.convergent.cdvcs`)
   needs: a content-addressed commit value + the canonical shared base. Pure, tiny, and
   store-free — the rest of the value-level CDVCS algebra (the pure `graph`/`core`
   reference) lives in the TEST tree as a correctness oracle, not in the shipped library.

   NAMED `builder`, NOT `commit`: a child ns `…cdvcs.commit` munges to the SAME
   ClojureScript JS path as the parent's `commit` VAR (`cdvcs/commit` → `…cdvcs.commit`),
   so on cljs the var assignment CLOBBERS the namespace object (vars + namespaces share
   one JS object graph) — wiping `make-commit`/`new-base`. Keep this name var-collision-free."
  (:require [hasch.core :as hasch]))

(defn- commit-id [commit-value]
  (hasch/uuid (select-keys commit-value #{:transactions :parents})))

(defn make-commit
  "Build a single commit value + its content-addressed id (`hash {:transactions
   :parents}`). Pure — the durable layer persists the blob and appends `[id parents]`
   to the commit-graph. Used by the PSS-backed (Option B) durable wrapper.

   TIME-FREE by design: the commit value carries NO wall-clock timestamp. A wall-clock
   `:ts` would make the SAME logical commit byte-DIFFERENT across peers (each stamps its
   own now), diverging the content-addressed graph leaf/root even though the commit-id
   (which excludes it) converges — killing cross-peer node dedup + determinism. Provenance
   that must converge belongs in the transactions (a logical/HLC stamp), not a raw clock."
  [author parents transactions]
  (let [cval {:transactions transactions :parents (vec parents) :crdt :cdvcs
              :version 1 :author author}]
    {:id (commit-id cval) :value cval}))

(defn new-base
  "The base commit (no transactions, no parents) — a CANONICAL shared sentinel:
   author-independent in BOTH id AND value (`:author nil`), so ALL fresh CDVCS share ONE
   byte-identical base ⇒ a common ancestor ⇒ always joinable. Value-determinism matters
   once commit values are inlined in the graph PSS: identical base value ⇒ identical leaf
   ⇒ identical content-addressed root across peers. Like every commit it is TIME-FREE (no
   `:ts`). `author` is ignored — the base is unattributed by design."
  [_author]
  (let [cval {:transactions [] :parents [] :crdt :cdvcs :version 1 :author nil}]
    {:id (commit-id cval) :value cval}))
