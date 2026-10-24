# Historical isolated workspaces

`Snapshotable` identifies historical states. `Branchable` names references.
Neither alone promises that checkout creates an independent writable workspace:
some adapters check out a logical branch in the same mutable connection.

`overlay-at` is the explicit historical workspace operation:

```clojure
(let [child (p/overlay-at system snapshot {:sync? true})]
  ;; use overlay-system to access the private writable system
  ;; merge-down! explicitly adopts work; discard! releases the workspace
  )
```

It requires positive `:snapshot-overlay true` capability as well as the
Snapshotable, Overlayable and SystemIdentity protocols. Unsupported systems
refuse; no fallback to today's state or to the original shared system exists.
Following/gated history requests refuse: a historical workspace is frozen.
The operation retains the ordinary value-or-CPS per-call execution convention;
callers must consume the actual completion, including disposal completion.

Supporting adapters receive `:snapshot` in their overlay constructor. Callers
must not pass that option directly to an arbitrary older adapter: unknown
options may be ignored. Use the guarded operation instead.

## Laws

1. Resolve the requested historical identity to a fixed native base. Child
   reads and `base-ref` must describe that base, even if the parent advances.
2. The original workspace's bytes, staging/index, selection and configuration
   must not be changed by constructing or discarding the child. A physical
   branch inventory may grow while its owned workspace exists.
3. Writes in the child cannot change the parent or a sibling. Explicit adoption
   uses the correct historical merge base, not a later parent head substituted
   as the base of the child's delta.
4. Construction, merge and discard refer to the same acquired resource. Do not
   allocate workspace F, reseat an opaque record onto H, then dispose F.
5. Failure after acknowledged acquisition cleans only the owned resources.
   Failure/uncertainty during allocation or cleanup must retain the exact
   resource identity for reconciliation; it is not acknowledged disposal.
6. The descriptor is state provenance, not spending or access authority. An
   embedder still checks current permissions and grants resources separately.

## Snapshot scope and savepoints

A Geschichte logical snapshot restores its committed tree. Ordinary physical
workspace forks can also carry dirty worktree and index state. These are
different capture scopes; this API does not redefine logical snapshot IDs or
silently commit the user's workspace.

Spindel portable savepoints require a stronger equivalence between live-fork
continuation and persisted/hydrated continuation. Full dirty-workspace fidelity
therefore needs an explicit retained workspace image, with GC roots and capture
coordination. A vector of unrelated substrate snapshot reads is not automatically
a coherent cross-system cut, and sequential publication is not a cross-store
all-or-nothing transaction.

Regression obligations include A-to-C parent advancement while the historical
child reads A, independent child writes, stable base/diff, exact resource
disposal, missing-snapshot refusal, creation-failure cleanup and nested forks.

## Native Datahike cleanup ownership

Ordinary and historical Datahike/Geschichte overlays acquire their own lease
on the protected physical `:db` branch before allocating a private branch.
This lease is for native cleanup only: it is not a selected application
workspace, write permission, logical Git checkout or newly created database.
Its effective store identity must match the source. Captured configuration,
connections and phase witnesses stay in an opaque process-local cleanup owner.

Cleanup releases the creation-owned child connection, removes its physical
branch through the owned canonical connection, then releases that canonical
lease. It never force-releases a shared connection. It does not borrow the
temporary parent's connection: a copied world's source may already have merged
and been disposed when another copy is discarded. Reseating overlay writes
does not transfer ownership. Datahike merge-down uses its captured parent and
does not acquire an additional unowned checkout lease.

A throwing branch allocation is resolved by reading the branch registry
through the owned canonical lease, not inferred from the exception. A
registered branch was allocated and is disposed like any other; an
unregistered one was not, and the lease is released. Either way the native
error propagates. This relies on a returned native call having no write still
in flight. Only a failed registry read leaves the allocation uncertain.

Successful cleanup is idempotent. A concurrent cleanup cannot dispatch another
release. Each native operation is marked in-flight before dispatch; a missing
acknowledgement retains the opaque owner and safe branch/phase locators, and
refuses automatic replay. For example, acknowledged child release followed by
uncertain branch removal retains the canonical cleanup lease. Native owners in
internal exception data are not portable receipts and must never be serialized
or exposed to an interpreter. Explicit operator reconciliation of uncertainty
is separate; these APIs do not infer rollback or success from an exception.
