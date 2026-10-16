## Why

Every dispatcher response passes through `ResponseConverter.convert`. It decides which keys a row keeps, and it recomputes that decision for every key of every row. Each check scans the transfer object's structural features against each other and does an EMF `default` annotation lookup on every inner step, so one key costs O(F²). In a downstream application, a 202-row list on an 86-member transfer object took about 4 s, of which about 0.1 ms was SQL. All 30 jstack samples were in this check, and an unmasked `~get` of 2 instances took about 19 s. The inputs depend only on the `EClass`, so the per-key work is redundant. The code has not changed since the repository's initial commit (783f6b83, JNG-6420).

## What Changes

- `ResponseConverter` computes the **allowed (public) feature names** of a type once per type and reuses them, instead of rebuilding them for every key of every row.
  - Allowed names = all `getEAllStructuralFeatures()` names, minus names that some feature of the same type references through its `default` extension annotation (the hidden `_<field>_default_<container>` companions).
  - The set is built in one O(F) pass: collect the `default` annotation targets, then filter the feature names. This replaces the O(F²) nested scan.
  - The cache is a `Map<EClass, Set<String>>` local to one `convert(...)` invocation. It is shared by every row and every nested embedded type in that response, and no state outlives the call. A `ResponseConverter` is built per dispatcher call (`DefaultDispatcher:419`), so there is no lifecycle or invalidation concern.
- The keep/remove rule stays as it is: a key is kept iff (`allowedNames(type)` contains it **or** `keepProperties` contains it) **and** its value is non-null.
- New **characterization tests** (`ResponseConverterTest`) pin the current behaviour. They are green both before and after the change. They cover:
  - declared features are kept
  - unknown keys are removed
  - null values are removed, including null `keepProperties`
  - `keepProperties` are kept even when they are not features
  - default-value companions are hidden
  - repeated conversions apply the same rules
  - nested embedded payloads use the nested type's own rules
- No public API, response content or configuration changes. **Not breaking.**

### Non-goals (observed, explicitly out of scope)

- `convertEnumerationValue` does an `asmUtils.all(EEnum.class)` linear scan for every enum value.
- The same one-way companion scan appears in `QueryFactory:305/458` (setup time only), and default checks appear in `PayloadDaoProcessor:212/218` and `RdbmsDAOImpl:164/175`. None of these are profiled.
- No cross-call or global cache of allowed names, so there are no model-reload or invalidation semantics to add.
- No change to how psm2asm models companions (no marker annotation on the companion itself).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `dispatcher`: the **Response Payload Conversion** requirement gains explicit scenarios for response key filtering: kept features, unknown keys, nulls, `keepProperties`, default-value companions and nested embedded types. It also gains a requirement that the allowed-name decision is derived once per type per conversion, not per key per row. The observable behaviour is unchanged. The spec currently describes "structural feature filtering" only in the Architecture prose, with no scenario.

## Impact

- **Code**: `judo-runtime-core-dispatcher`
  - `ResponseConverter.java`: a per-call `allowedNamesByType` map and a private static `allowedNames(EClass)` helper, about 20 lines.
  - `ResponseConverterTest.java` (new): 8 characterization tests built on an in-memory ASM model with `EcoreBuilders`.
- **APIs / dependencies**: none.
- **Performance**: per-row cost goes from O(keys × F²) annotation lookups to O(keys) hash lookups, plus one O(F) build per distinct type per response. In a local experiment the 202-row / 86-member list went from about 4,062 ms to about 44 ms with identical content. A 15×20 composite create went from 40.4 s to 15.5 s.
- **Risk**: low. The predicate is logically equivalent. Old: `f` is allowed iff no `d` in `eAllStructuralFeatures` has `default == f.name`. New: `f.name ∉ {default(d) | d ∈ eAllStructuralFeatures}`. Edge cases behave the same: self-reference, the `"-"` sentinel for missing annotations, and duplicate names across supertypes. The evaluation order is unchanged, so the null check still runs last. That ordering is now cheap, so no reordering is needed.
- **Verification**:
  - `ResponseConverterTest`, green before and after.
  - The full `judo-runtime-core-dispatcher` suite, green.
  - Downstream application backend and E2E suites with no new failures.
  - A re-measure of the 202-row probe.
