# Categorical Semantics of Yggdrasil

> **Note**: This document is optional reading for users interested in the formal mathematical foundations of Yggdrasil. It is not required to use the library. See [README.md](../README.md) for practical usage.

Yggdrasil's protocols can be understood through the lens of category theory, specifically as a **snapshot-first model** where version control operations correspond to categorical constructions.

This document establishes the formal semantics, explains the relationship to patch-based version control systems (Darcs, Pijul), and justifies key design decisions.

---

## 1. The Category of Snapshots

### Definition

Yggdrasil systems form a category **C_Snapshots**:

- **Objects**: Snapshots (immutable COW states)
- **Morphisms**: Implicit deltas between snapshots
- **Composition**: Transitive state changes (chaining deltas)
- **Identity**: Empty delta (snapshot to itself)

### Why Snapshot-First?

For COW (copy-on-write) systems, snapshots are primitive and cheap:

| System | Snapshot Cost | Delta Cost |
|--------|---------------|------------|
| Git | O(1) - commit hash | O(n) - tree diff traversal |
| ZFS | O(1) - pointer to block tree | O(n) - block scan |
| IPFS | O(1) - content CID | O(n) - DAG traversal |
| Datahike | O(1) - database value | O(n) - datom comparison |
| Iceberg | O(1) - metadata pointer | O(n) - manifest comparison |

**Conclusion**: Storing snapshots and computing deltas on-demand is the natural approach for these systems.

### Category Axioms

**Identity**:
```
id_s : s → s
For any snapshot s, the empty delta exists
```

**Composition**:
```
Given f: s₁ → s₂ and g: s₂ → s₃
Then g ∘ f : s₁ → s₃ exists
```

**Associativity**:
```
(h ∘ g) ∘ f = h ∘ (g ∘ f)
```

In Yggdrasil terms: The final state after a sequence of commits is independent of how you group them.

---

## 2. Pushout as Merge

### The Merge Problem

Given a fork:
```
      s₀ (base)
      /  \
    f/    \g
    /      \
   s₁      s₂
```

How do we construct a merged state `m` that incorporates both changes?

### Pushout Definition

A **pushout** is an object `m` with morphisms `f': s₁ → m` and `g': s₂ → m` satisfying:

**1. Square Commutes**:
```
    s₀ ---g---> s₂
    |           |
    f          g'
    |           |
    v           v
    s₁ ---f'---> m
```

Both paths from s₀ to m represent the same composite change.

**2. Universal Property**:

For any other candidate merge `n` with morphisms `h: s₁ → n` and `k: s₂ → n` that also commute, there exists a **unique** morphism `u: m → n` such that:
- `u ∘ f' = h`
- `u ∘ g' = k`

**Meaning**: `m` is the **most general merge** - every other merge can be obtained from `m` by additional transformations.

### Example: Git Merge

**Base commit**:
```bash
git init
echo "hello" > readme.txt
git add . && git commit -m "base"
# Snapshot s₀ = abc123
```

**Branch A** (main):
```bash
git checkout -b main
echo "world" >> readme.txt
git commit -am "add world"
# Snapshot s₁ = def456
```

**Branch B** (feature):
```bash
git checkout -b feature abc123
echo "test" > test.txt
git commit -am "add test"
# Snapshot s₂ = ghi789
```

**Merge** (pushout):
```bash
git checkout main
git merge feature
# Git constructs pushout automatically:
# - Finds common base (s₀)
# - Applies both changes
# - Creates merge commit m with parents [s₁, s₂]
# Snapshot m = jkl012
```

**Categorical structure**:
- Object s₀: `{readme.txt: "hello"}`
- Object s₁: `{readme.txt: "hello\nworld"}`
- Object s₂: `{readme.txt: "hello", test.txt: "test"}`
- Object m: `{readme.txt: "hello\nworld", test.txt: "test"}`

Morphisms:
- f: s₀ → s₁ = "append 'world' to readme.txt"
- g: s₀ → s₂ = "add test.txt"
- f': s₁ → m = "add test.txt" (apply g's changes)
- g': s₂ → m = "append 'world' to readme.txt" (apply f's changes)

Square commutes: `g' ∘ g = f' ∘ f` (both paths reach same state m)

### Example: Datahike Merge

**Base state**:
```clojure
(require '[datahike.api :as d])

(def conn (d/connect cfg))
@conn  ; Database value s₀

(d/transact conn [{:db/id -1 :name "Alice"}])
; Snapshot s₀ = #datahike/DB{...}
```

**Branch A**:
```clojure
(d/branch conn :main)
(d/transact conn [{:db/id -1 :name "Bob"}])
; Snapshot s₁
```

**Branch B**:
```clojure
(d/branch conn :feature)
(d/transact conn [{:db/id -2 :age 30}])
; Snapshot s₂
```

**Merge**:
```clojure
(d/merge conn :feature)
; Datahike constructs pushout:
; - Both transactions are independent (different entities)
; - Result includes both changes
; Snapshot m with parent-ids [s₁, s₂]
```

**Categorical structure**:
- Object s₀: `#{[:name "Alice"]}`
- Object s₁: `#{[:name "Alice"] [:name "Bob"]}`
- Object s₂: `#{[:name "Alice"] [:age 30]}`
- Object m: `#{[:name "Alice"] [:name "Bob"] [:age 30]}`

Morphisms:
- f: s₀ → s₁ = "add [:name 'Bob']"
- g: s₀ → s₂ = "add [:age 30]"
- f': s₁ → m = "add [:age 30]"
- g': s₂ → m = "add [:name 'Bob']"

### Manual Pushout Construction (Yggdrasil)

For systems where Yggdrasil doesn't know domain semantics (IPFS, Iceberg, ZFS), the user provides the pushout:

```clojure
(require '[yggdrasil.adapters.ipfs :as ipfs])

(def sys (ipfs/init! {:system-name "dataset"}))

;; Base
(ipfs/commit! sys "base" {:root "QmBase..."})

;; Fork
(p/branch! sys :main)
(p/branch! sys :feature)

(p/checkout sys :main)
(ipfs/commit! sys "main work" {:root "QmMain..."})  ; s₁

(p/checkout sys :feature)
(ipfs/commit! sys "feature work" {:root "QmFeature..."})  ; s₂

;; User constructs pushout manually
;; $ merge-tool QmMain... QmFeature... => QmMerged...

;; Yggdrasil records the pushout
(p/checkout sys :main)
(p/merge! sys :feature {:root "QmMerged..." :message "Merge feature"})
; Creates commit m with parent-ids [s₁, s₂]
```

**Why this is valid**:

The pushout is defined by its **universal property**, not its construction algorithm. As long as:
1. The merged state incorporates both changes
2. Both parent-ids are recorded
3. The square commutes (both paths reach the same merged state)

...the universal property is satisfied, regardless of whether Git computed it or a human did.

---

## 3. Comparison with Patch-First Models

### Darcs & Pijul

**Darcs** (Roundy, 2005) and **Pijul** (Nest et al., 2020) use a **patch-first** categorical model:

- **Objects**: File states (derived by applying patches)
- **Morphisms**: Patches (primitive operations - add line, delete file, etc.)
- **Composition**: Sequential patch application
- **Merge**: Automatic pushout via **patch commutation**

**Key property**: Two patches `p` and `q` **commute** if swapping their order produces an equivalent result:
```
apply q (apply p state) = apply p' (apply q state)
```

When patches commute, merge is automatic. When they don't → conflict.

### Yggdrasil vs Pijul

| Aspect | Pijul (Patch-First) | Yggdrasil (Snapshot-First) |
|--------|---------------------|----------------------------|
| **Objects** | Derived (apply patches to empty) | Primitive (COW snapshots) |
| **Morphisms** | Primitive (stored patches) | Derived (compute deltas) |
| **Domain** | Text files, source code | Large-scale COW systems |
| **Merge** | Automatic (commute patches) | Manual (user provides pushout) |
| **Commutation** | Required for auto-merge | Rare, not required |
| **Examples** | Line edits, refactorings | ZFS datasets, IPFS CIDs, Iceberg tables |

### Why Commutativity is Rare

**Example**: Database schema changes
```sql
-- Base: users table with [id, name]

-- Change A: Add email column
ALTER TABLE users ADD COLUMN email TEXT;

-- Change B: Insert user
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- Do A and B commute?
-- Order 1 (A then B): Row has NULL email ✓
-- Order 2 (B then A): May fail (depends on DB) ✗

-- Conclusion: A and B do NOT commute
```

For large-scale systems (databases, filesystems, data lakes), changes are often **temporal** and **order-dependent**. Commutativity is the exception, not the rule.

Yggdrasil accepts this: snapshots encode total order, and merge is an explicit manual operation.

### Both Models are Valid

**Category theory perspective**:

- Pijul: Category **C_Patches** (morphisms are patches)
- Yggdrasil: Category **C_Snapshots** (objects are snapshots)

These are **dual perspectives** on the same problem. The choice depends on domain:
- **Small, independent edits** (source code) → Patch-first (Pijul)
- **Large, dependent changes** (COW systems) → Snapshot-first (Yggdrasil)

---

## 4. Protocol Mapping to Category Theory

### Snapshotable Protocol

Maps to **object structure**:

```clojure
(snapshot-id sys)      ; Current object in category
(parent-ids sys)       ; Source objects of incoming morphisms
(as-of sys snap-id)    ; Object lookup
(snapshot-meta sys id) ; Object metadata
```

### Branchable Protocol

Maps to **coinitial morphisms** (parallel changes from common base):

```clojure
(branch! sys :feature from-snap)
; Creates:
;       from-snap
;          /  \
;   :main /    \ :feature
;        /      \
;      s_m      s_f
```

Both `:main` and `:feature` start from `from-snap`, creating parallel morphisms.

### Graphable Protocol

Maps to **category structure**:

```clojure
(history sys)           ; Morphism chain from current object
(ancestors sys snap)    ; Transitive closure of incoming morphisms
(commit-graph sys)      ; Full category: objects + morphisms + branches
```

### Mergeable Protocol

Maps to **pushout construction**:

```clojure
(merge! sys :feature opts)
; Given:
;       base
;       /  \
;   f  /    \ g
;     /      \
;    A        B
;
; Creates pushout:
;    A        B
;     \      /
;   f' \    / g'
;       \  /
;        M (with parent-ids [A, B])
```

For adapters where Yggdrasil knows semantics (Git, Datahike), merge may be automatic. For others (IPFS, ZFS, Iceberg), user provides the merged state via `:root` or similar.

---

## 5. Implications for Design

### Why Current Protocols are Sound

1. **Snapshotable**: Defines objects in the category ✓
2. **Branchable**: Enables fork (coinitial morphisms) ✓
3. **Graphable**: Exposes category structure (DAG) ✓
4. **Mergeable**: Implements pushout ✓

All fundamental categorical constructions are supported.

### Why Optional Protocols are Truly Optional

**Commutable**: Only makes sense for adapters that can detect independent changes
- Git: File-level independence
- Datahike: Transaction independence
- ZFS, IPFS, Iceberg: Changes are temporal, don't commute

**Revertable**: Only for systems with true inverse operations
- Git: `git revert` (creates inverse commit)
- Datahike: Transaction retraction
- ZFS: Rollback to snapshot (destructive, not a morphism)

**Patchable**: Only for systems where computing deltas is cheap
- Git: Tree diff is relatively fast
- ZFS, IPFS: Block/DAG scan is expensive
- Better to compute on-demand when needed

These protocols may be added in the future for specific adapters, but are not required for categorical soundness.

### Manual Merge is Not a Limitation

Some might view manual merge (user provides `:root`) as a limitation compared to automatic merge (Git, Pijul).

**Categorical perspective**: Both are equally valid pushout constructions.

**Practical perspective**: For large-scale systems, domain-specific merge tools are better than generic algorithms:
- Iceberg: Spark SQL handles schema conflicts
- IPFS: Dataset-specific merge logic
- ZFS: Administrator decides which dataset is canonical

Yggdrasil's role is to **track the merge in the DAG**, not to compute it.

---

## 6. References

### Patch Theory

- **Darcs**: Roundy, D. (2005). "Darcs Patch Theory". Available in Darcs documentation.
- **Pijul**: Nest et al. (2020). "A Categorical Theory of Patches". Pijul Theory Manual: https://pijul.org/manual/theory.html

### Categorical Theory of Patches

- **Mimram, S., & Di Giusto, C.** (2013). "A Categorical Theory of Patches". *arXiv:1311.3903*. Formal categorical framework for version control.
- **Angiuli, C., Morehouse, E., Licata, D. R., & Harper, R.** (2016). "Homotopical Patch Theory". Extended abstract with higher-categorical perspective.

### Category Theory Background

- **Mac Lane, S.** (1978). *Categories for the Working Mathematician*. Springer. Standard reference for category theory.
- **Awodey, S.** (2010). *Category Theory* (2nd ed). Oxford University Press. Accessible introduction.

### COW Systems

- **Git**: https://git-scm.com/book/en/v2/Git-Internals-Plumbing-and-Porcelain
- **ZFS**: Rodeh, O., Bacik, J., & Mason, C. (2013). "BTRFS: The Linux B-Tree Filesystem". *ACM TOS*.
- **IPFS**: https://docs.ipfs.tech/concepts/
- **Datahike**: https://github.com/replikativ/datahike

---

## Summary

Yggdrasil implements a **snapshot-first categorical model** appropriate for large-scale heterogeneous COW systems:

- **Objects**: Snapshots (primitive, cheap to create)
- **Morphisms**: Deltas (derived, expensive to compute)
- **Merge**: Pushout (manual construction by user/tools)
- **Soundness**: Satisfies all category axioms and universal properties

This differs from **patch-first models** (Darcs, Pijul) where patches are primitive and snapshots are derived. Both approaches are categorically valid; the choice depends on domain characteristics.

For practical usage, see [README.md](../README.md). For implementation details, see the source code in `src/yggdrasil/`.
