## Context

See proposal.md (Why) for the motivation and the measurements.

`ResponseConverter.convert(Map)` builds a `PayloadTraverser`. The traverser calls a `processor(instance, ctx)` lambda once per row, and it recurses into embedded references (`AsmUtils.isEmbedded`), giving each nested row its own `ctx.getType()`. The lambda first converts attribute values (enum, binary, coercion). It then iterates `instance.entrySet()` and removes a key when this holds:

```
(!publicMember(type, key) && !keepProperties.contains(key)) || value == null
```

Before this change, `publicMember(type, key)` was evaluated inline as a nested stream:

```
anyMatch over f in eAllStructuralFeatures
  where noneMatch over d in eAllStructuralFeatures: default(d) == f.name
  and f.name == key
```

`default(d)` is `AsmUtils.getExtensionAnnotationValue(d, "default", false).orElse("-")`, which goes through the `AsmUtilsCache` (a Guava cache keyed by a new `Pair`, plus an `eResource()` walk). The evaluation runs per key per row, costing O(F²) annotation lookups each time.

Constraints:
- `DefaultDispatcher` builds `ResponseConverter` per call (`DefaultDispatcher:419`), and `convert` is called once per response.
- The companion link is one-way: only the real feature carries the `default` annotation, and the companion has no marker of its own (see psm2asm `transferObject.etl`).
- Response content must not change (see specs/dispatcher/spec.md).

## Goals / Non-Goals

**Goals:**
- Evaluate `publicMember(type, ·)` with one O(F) derivation per distinct type per `convert` call, and an O(1) lookup per key.
- Keep the filter predicate logically identical, including the evaluation order of the `||` / `&&` terms.
- Keep the change local to `ResponseConverter`, with no new types, no API change and no new dependency.

**Non-Goals:**
- Caching across `convert` calls or across dispatcher calls (e.g. in `DefaultDispatcher` or `AsmUtils`).
- Changing psm2asm to mark companions directly.
- The other hot spots listed in proposal.md (Non-goals): `convertEnumerationValue`, `QueryFactory`, `PayloadDaoProcessor`, `RdbmsDAOImpl`.

## Decisions

### D1: Precompute the public member names as a `Set<String>` per `EClass`

`allowedNames(EClass type)` (private static) works in two passes:
1. `defaultCarriers = { default(d) | d ∈ eAllStructuralFeatures }`. Features without the annotation contribute the `"-"` sentinel, exactly as before.
2. `allowed = { f.name | f ∈ eAllStructuralFeatures, f.name ∉ defaultCarriers }`, returned unmodifiable.

Equivalence: the old code allowed `f` iff `¬∃d: default(d) = f.name`, which is exactly `f.name ∉ defaultCarriers`. The `anyMatch(f.name == key)` then becomes `allowed.contains(key)`. If two features share a name (e.g. inherited from more than one supertype), they collapse into one set entry with the same answer as before.

*Alternative considered: a `Set<EStructuralFeature>` of companions, then a name match.* It needs an extra hop per key and gains nothing, because keys are names.

*Alternative considered: reordering the condition to test `value == null` first.* That would save the lookup for null keys. It was rejected as unnecessary once the lookup is O(1), and keeping the original order makes equivalence easier to review.

### D2: Cache scope = one `convert(...)` invocation

A `Map<EClass, Set<String>> allowedNamesByType = new HashMap<>()` is created inside `convert`, before the traverser is built. The processor lambda captures it and calls `computeIfAbsent(ctx.getType(), ResponseConverter::allowedNames)`.

- This covers every row and every nested embedded type of one response. That's where the multiplier is: rows × keys.
- Nothing outlives the call, so there are no invalidation, model-reload or memory-growth concerns. That matches the "no state retained" scenario in the spec.
- A plain `HashMap` is enough because `PayloadTraverser` processes one payload on the calling thread.

*Alternative considered: a field on `ResponseConverter`.* It would be equivalent today, since there is one instance per dispatcher call, but it would silently start sharing state if a caller reused a converter. The per-call local keeps the guarantee explicit.

*Alternative considered: a global `ConcurrentHashMap<EClass, Set<String>>` (or `AsmUtils`-level cache).* It would also remove the one-time O(F) per response. It was rejected because it brings lifecycle questions (model reloads, `EClass` identity, unbounded growth) for a cost that is already negligible after D2.

### D3: Characterization tests before the change (TDD)

`ResponseConverterTest` builds an in-memory ASM model with `EcoreBuilders` and `AsmModelResourceSupport`, and uses a pass-through `Coercer` mock. It pins every scenario in specs/dispatcher/spec.md, including a nested embedded type with its own companion. The suite must be green on the original code, which proves it characterizes current behaviour, and green after the change. That was verified by stashing the implementation and re-running the suite.

## Risks / Trade-offs

- [Behavioural drift in an edge case not covered by the tests] → The predicate is proven equivalent (D1). The characterization tests cover companions, keep properties, nulls, unknown keys and nesting. Downstream backend and E2E suites are run as a final gate.
- [The per-response O(F) build still repeats across requests] → It's accepted. It is a single pass per distinct type, which is negligible next to the removed rows × keys × F² term. A cross-call cache can be a follow-up if profiling ever shows it.
- [The measured gain comes from one downstream application] → The acceptance target is relative (same order as narrow transfer objects), and the 202-row probe is re-measured after the change.

## Migration Plan

There's no migration: no API, configuration or data change. To roll back, revert the `ResponseConverter` hunk. The characterization tests stay valid against either version.
