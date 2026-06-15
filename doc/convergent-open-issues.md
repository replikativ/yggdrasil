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
