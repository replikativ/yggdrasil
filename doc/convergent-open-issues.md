# Convergent CRDT subsystem — open issues, decisions & the value-semantics plan

Status tracker for the conflict-free / durable-CRDT layer (`yggdrasil.convergent.*`),
produced by a 3-pass review (CRDT correctness → cross-platform symmetry → state &
memory-model soundness), 2026-06. Keep this current as items land.

## Headline decision (DECIDED): move to value semantics, minimize mutable state

The catalog is uniformly **conn-style** today: every mutator (`system/put!`/`upd!`,
durable `add`/`remove-elem`/`apply-delta`/`conj-into!`/`merge-peer!`) mutates internal
atoms IN PLACE (`swap!`/`reset!`) and returns the same handle (with δ in metadata);
only `-join` is pure (fresh handle on change, `this` on no-op). This is **sound as
used** — spindel fires observers off a monotonic *generation* counter, not value
equality, and re-publish suppression uses `identical?` on the fresh handle `-join`
returns — but **fragile**: the existing `swap-signal-changed?` path or any
equality-gated consumer would silently drop mutations, and a `deref` is not a
snapshot (it aliases live mutable atoms). It also contradicts the protocol contract
(`protocols.cljc:4-6`: "mutating operations return a new system value rather than
modifying in place").

**Decision:** convert the catalog to **value semantics** — records hold plain
immutable fields (no internal atoms); every mutator returns a NEW handle. The ONLY
mutable cells left are the *necessary* boundaries: the spindel **signal-atom** (the FRP
write boundary), the **konserve store** (I/O), and PSS's storage-adapter **freed-set**
(GC bookkeeping inside `KonserveStorage`). See the conversion plan at the bottom.

---

## Fixed this session (committed/uncommitted on `feat/cljc`)

| # | Severity | Area | Fix |
|---|---|---|---|
| F1 | major | state/race | `save-roots!`/`save-freed!` get-then-assoc TOCTOU → atomic konserve `update` (new `kbridge/k-update`). No lost branch under concurrent flush / synced-peer write. |
| F2 | major | state/data-loss | `gc!` reclaimed held `as-of`/frozen snapshots. Wired the ignored `snapshot-ids` param through `gc-sweep!` → `gc!` `:retain-roots`/`:retain-keys` (gset: id=root; 2p/or: id=commit→both halves). + regression test `gc-retains-held-snapshot`. |
| F3 | minor | CRDT contract | 2P-Set/OR-Set advertised `:branchable/:mergeable true` but no-op them (composite cap-agg + compliance dispatch on it) → `false`. |
| F4 | minor | CRDT doc | OR-Map: documented the deterministic-tag commutativity precondition (uid must be value-inclusive; `merge-with merge` is right-biased on collision). |
| F5 | major | cross-platform | `convergent/composite.clj` → `.cljc` (only clj-only convergent leaf; nothing blocked it). Browser peers can now `-join` composites. Added to cljs test require. |
| F6 | minor | cross-platform determinism | LWWR tie-break `pr-str` → `hasch/uuid` (pr-str of maps/sets is iteration-order-dependent → replicas could pick different winners). |
| F7 | minor | cross-platform determinism | `types/logical-max` `Integer/MAX_VALUE`-vs-`MAX_SAFE_INTEGER` → single shared literal (identical `as-of` ceiling HLC; no JVM-int overflow). |
| F8 | minor | cross-platform | `workspace/gc!` cljs branch `nil` (silent nil-call NPE) → legible "JVM-only" throw. |

Verified: JVM convergent suite 48 tests / 168 assertions green; shadow (current-cljs)
build rc=0 (all changes + composite.cljc compile on cljs).

---

## Open decisions (semantic — need a human call)

- **D1 — LWWR wall-clock → HLC.** Tie-break determinism is now fixed (F6), but LWWR
  still stamps with `types/now-ms` (wall-clock, no node id) → a stale write wins under
  clock skew, and same-ms concurrent writes resolve by value-hash not by a per-replica
  id. The codebase HAS an HLC (`types/->HLC`, used by the registry). Recommend stamping
  LWWR with an HLC `[physical logical node-id]` — but it changes register semantics, so
  it's a deliberate choice. (Pass A + B.)
- **D2 — δ on a no-op `-join`.** The no-op branch returns `this` *with* its local δ
  while the changed branch returns a fresh δ-free handle (inconsistent post-condition).
  Verified **sound as used** (the signal layer clears δ at the export boundary, so a
  retained δ never double-propagates), so left as-is — but if op-streaming ever runs
  `-join` while a local δ is un-exported, decide whether the result should preserve the
  receiver's own δ or drop it. Pin the intended contract with a test. (Pass A.)

## Deferred (tied to other work)

- **Df1 — `set-union` is O(|b|) conj, not a structural Merkle merge**; `durable_gset`
  recomputes its dirty-set by a full `=`-scan. Both are resolved by the **prolly /
  content-defined-chunking structural-merge** follow-up — see
  `persistent-sorted-set/doc/prolly-tree-requirements.md`. Not fixable in yggdrasil
  alone. (Pass A.)
- **Df2 — lazy-read vs concurrent GC.** A lazy PSS traversal can hit a GC'd node if
  reads and GC aren't serialized; mitigated by single-writer-per-store but **not
  enforced in-process**. Fold into the single-writer-enforcement work (the same
  invariant D2/the durable mutators assume). (Pass C.)

## Test-coverage gaps (not bugs)

- **T1 — PARITY-UNTESTED on cljs.** `durable_orset` / `durable_2pset` multi-root async
  (`as-of`/`read-commit`), `durable/set-union`/`ship!`/`reachable-addresses` async
  drain, and `registry`/`workspace`/`overlay` async branches have NO cljs test
  exercising them (parity asserted only on JVM). Add shadow node-tests. (Pass B.)
- **T2 — bare `:cljs-test` build is broken** on konserve's `cljs.core/resolve`
  macroexpansion under the old cljs pin (pre-existing; documented in `deps.edn:31-33`).
  The real cljs path is `:shadow` (current cljs). Either drop the konserve force-require
  from `yggdrasil.convergent.cljs-test` or retire the `:cljs-test` alias. (Pass B.)

---

## The value-semantics conversion — how

Target: CRDT records hold **plain immutable fields**, mutators return **new handles**,
`-join` unchanged. Remove every internal atom from the CRDT records. Keep mutable state
ONLY at the signal-atom (FRP), konserve store (I/O), and PSS freed-set (storage GC).

Phased so each phase compiles + passes its tests on its own:

1. **In-mem tier** (`system.cljc` + `gset`/`lwwr`/`ormap`). `store` atom → plain map
   field. `cur` reads the field; `put!`/`upd!`/`branch!`/`merge!` return `(assoc this
   :store …)`. `-join` already constructs fresh — drop the atom wrapper. Lowest risk
   (no I/O, self-contained). Verify with `gset/lwwr/ormap` + the generic system tests.

2. **Durable tier** (`durable_gset`/`durable_2pset`/`durable_orset`). `adds-atom`/
   `removals-atom`/`dirty-atom` → plain fields `adds`/`removals`/`dirty`. Every
   `@(:x-atom s)` → `(:x s)`; every `(reset! (:x-atom s) v)` → return `(assoc s :x v
   :dirty true)`. Restructure the **internal multi-step loops** to thread the handle:
   `add-all` (2pset) and `remove-elem`'s tombstone loop currently rely on in-place
   mutation of one handle — rewrite to compute the final sets purely then build one new
   handle. `flush!` returns `(assoc s :dirty false)` after persisting; **callers must
   adopt the returned handle** (see phase 4). `merge-peer!`/`apply-delta` return new
   handles. `-join` constructs fresh without atoms.

3. **Overlay + composite.** `overlay`'s `:frozen` clone `(atom @adds-atom)` → carry the
   value directly; `:following` empty-delta atoms → empty-value fields. `composite`
   already holds sub-systems as records (its `-join` is pure) — just confirm no atom
   reliance.

4. **Call-site adoption audit (the critical ripple).** Grep every caller of a mutator /
   `flush!` in yggdrasil AND spindel (`ygg_signal.cljc` `ygg-swap!`, `signal_sync.cljc`
   `apply-incoming!`). Confirm each **rebinds** to the returned handle (the signal-atom
   `swap!`/`reset!` already does; in-yggdrasil internal multi-step callers are the risk).
   The dirty-clearing in `flush!` MUST propagate — a caller that flushes and keeps the
   old handle would re-flush forever / never see clean.

5. **Re-verify.** yggdrasil convergent suite + shadow cljs build + **spindel's full
   suite (843 tests)** — signal_sync/ygg_signal consume these handles, so the
   conversion must be proven against the FRP layer, not just yggdrasil in isolation.

Risk notes: the `dirty` field is a *performance* optimization (skip redundant flush)
and is fine to keep as a plain value field (it's carried, not mutated). If threading it
proves noisy, the alternative is to drop it and always-flush (idempotent + content-
addressed → a clean flush only rewrites the small roots cell). The atoms are NOT needed
for performance (PSS nodes are immutable + shared), so removing them is pure cleanup.

### Phase 1 DONE (2026-06-15, ygg `11f9844` / spindel `ee6639c`)
In-mem tier value-semantic: `system.cljc`'s `store` atom → plain map; mutators return
new systems; `-join` pure over the map. The in-mem catalog is test-only — call-site
adoption was the conn-style "discard-return, re-read" pattern in gset/lwwr/ormap tests
+ spindel `composite_join_test` (now seeds via build-then-register). Full convergent
suite 51/178 green.

### Phase 2 REFINED SCOPE (the cascade — discovered while scoping)
The original plan under-scoped this. The durable CRDTs **cannot be converted in
isolation**: their *holders* are conn-style and discard the durable return values,
relying on in-place atom mutation:
- **registry** (`registry.cljc`) holds a `durable-2pset` in a plain field and does
  `(d2p/add (:tpset r) e)` / `(d2p/flush! (:tpset r))` discarding returns (`:75/:87/:93`).
- **composite** (`composite.cljc`) holds subs in a plain `{id→system}` map and mutates
  them in place via their atoms (sub `commit!`/`add`); `composite_cljs_test` mutates a
  `get-subsystem` handle in place.
- **workspace/gc** call `(reg/flush! registry)` (5+ sites) discarding the return.
- Many tests (durable_gset/2pset/orset, gc, roots, transactional, sync, the cljs ones,
  spindel `ygg_signal_test`) use the in-place add/flush pattern.

**Keystone model (the optimal resolution):** a CRDT is a pure immutable VALUE; its
HOLDERS are conns with ONE atom each that `swap!` the value:
- durable `gset`/`2pset`/`orset`: drop internal atoms → plain fields; mutators/`flush!`/
  `-join`/`apply-delta`/`merge-peer!` return new values; overlay carries values.
- **registry → a conn**: `[tpset-atom kv-store store-config]`; `register!`/`deregister!`/
  `add-all` `swap!` the atom with the value-semantic `d2p` result; `flush!`/`gc!` persist
  then swap. Its EXTERNAL API stays "mutate via the handle" → **workspace/gc callers
  unchanged**. Net: 3 atoms-in-value → 1 atom-in-service (fewer atoms, value CRDT).
- **composite → hold subs behind the same discipline** (sub mutations must be adopted
  back into `systems`; either a systems-atom conn or get-subsystem returns + re-seats).
- **overlay** (`yggdrasil.convergent.overlay`, the SHARED `ovl/convergent-overlay`/
  `overlay-system`/`merge-down!` machinery) is a FOURTH holder: it parks a system clone
  in `local-writes` (an atom) and `overlay-system` hands it out for in-place mutation;
  value-semantic CRDTs need `overlay-system` to return + re-seat, or an `overlay-swap!`.
- **spindel signal** already a conn (`ygg-swap!` adopts) — no change.

**No truly-independent per-CRDT increment exists** (an early hope): the durable CRDTs
SHARE the overlay machinery, and converting one CRDT's value semantics breaks its
overlay test (`orset-overlay-isolate-merge-down` mutates the clone in place) until the
overlay holder adopts. So Phase 2 is a COUPLED UNIT — durable values + the four holders
must land together.

**Sequence (one coupled change, staged for review but landed/verified together):**
- 2a. Convert the overlay holder (`overlay.cljc`) to adopt value-semantic clones
  (`overlay-system` re-seats / `merge-down!` joins the adopted value).
- 2b. Convert `durable-gset`/`durable-2pset`/`durable-orset` values (atoms → fields;
  mutators/`flush!`/`-join`/`apply-delta`/`merge-peer!` return new values).
- 2c. **registry → conn** (swaps the value-semantic `d2p`; external API stable →
  workspace/gc unchanged) + **composite** sub-adoption.
- 2d. Fix all in-place test call-sites (durable_gset/2pset/orset, gc, roots,
  transactional, sync, cljs, spindel `ygg_signal_test`) to thread/adopt.
- 2e. Full verify: ygg convergent+registry+workspace+composite + shadow cljs +
  **spindel 843-test suite** (the FRP consumers).

Phase 1 done. Phase 2 is the coupled durable+holders unit above — a focused effort, not
piecemeal (an isolated orset attempt was reverted because the shared overlay breaks).

### Phase 2 DONE (2026-06-15, ygg `9c53c3b`)
Durable tier (gset/2pset/orset) value-semantic (atoms → plain fields; mutators/flush!/
-join return new values). Holders became conns: overlay `overlay-swap!`, registry
`tpset-atom` (external API unchanged → workspace/gc untouched), composite
`update-subsystem`/`overlay-subsystem-swap!`. Tests threaded. Green: ygg JVM 77/295,
ygg cljs shadow 7/20, **spindel 843/2863**.

**New semantic (a deliberate casualty):** `:following` overlays no longer auto-track a
still-evolving parent — the overlay captures the parent BY VALUE, so live-tracking is
now the FRP **signal** layer's job (re-derive the overlay when the parent signal fires),
not the overlay layer. `:following` = parent-at-fork joined with the overlay's own
writes. (`:following` was deferred/thin/non-production, so this is acceptable; revisit
if a true live-tracking overlay is needed — it'd take the parent's signal-ref.)

**The catalog is now fully value-semantic.** Mutable state lives ONLY at the necessary
boundaries: the spindel signal-atom (FRP), the konserve store + PSS freed-set (I/O), and
the registry/overlay conn-atoms (service holders). The CRDT values carry none.
