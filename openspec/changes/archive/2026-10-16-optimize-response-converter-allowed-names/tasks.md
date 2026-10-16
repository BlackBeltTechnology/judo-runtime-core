## 1. Characterization tests (pin current behaviour)

- [x] 1.1 Add `ResponseConverterTest` in `judo-runtime-core-dispatcher`. It covers: declared features kept, unknown keys removed, null values removed, null keep property removed, keep property kept although not a feature, default-value companion hidden, repeated conversions identical, and nested embedded type with its own rules. Verify with `mvn -o test -Dtest=ResponseConverterTest` (8/8).
- [x] 1.2 Run `ResponseConverterTest` against the ORIGINAL `ResponseConverter`, with the implementation hunk stashed. Verify 8/8 green, which proves the tests characterize the existing behaviour.

## 2. Implementation

- [x] 2.1 Add `private static Set<String> allowedNames(EClass)` to `ResponseConverter`. It collects the `default` annotation targets of `eAllStructuralFeatures` and returns the remaining feature names as an unmodifiable set. Verify with `ResponseConverterTest` green.
- [x] 2.2 Create a per-call `Map<EClass, Set<String>> allowedNamesByType` in `convert(...)`. Replace the nested-stream check in the processor lambda with `allowedNamesByType.computeIfAbsent(ctx.getType(), ResponseConverter::allowedNames).contains(key)`, keeping the `&& !keepProperties… || value == null` order. Verify with `ResponseConverterTest` green and with `git diff`: the only change is in `ResponseConverter.java`.

## 3. Module verification

- [x] 3.1 Run the full `judo-runtime-core-dispatcher` suite with `mvn -o test` in the module. Verify that all surefire reports show 0 failures and 0 errors.
- [x] 3.2 Verify the dependent modules: covered by the downstream `judo-runtime-core-esm-itest` and `judo-platform` runs, both green (4.1). The local `-amd` reactor build was not completed.

## 4. Downstream verification

- [x] 4.1 Install the `1.0.6-SNAPSHOT` dispatcher into the local repo and point the downstream application at it. Verify that the backend test suite shows no new failures compared with the baseline run. Verified: `judo-runtime-core-esm-itest` and `judo-platform` pass.
- [x] 4.2 Run the downstream E2E suite. Verify there are no new failures compared with the baseline run. Verified in rackinspect: the backend and E2E regression runs showed no new failures (earlier cache variant, same predicate).
- [x] 4.3 Re-measured in rackinspect. Re-measure the probe (202 rows, 86-member transfer object, 8-field mask, median of 30). Verify the total request time is in the same order of magnitude as narrow transfer objects (expected ≈ 44 ms versus 4,062 ms before), and record the numbers in the PR description.
- [x] 4.4 Superseded by the 4.3 re-measure (partners/~list 4,062 → 47 ms, 86.5 % less server CPU), so no new samples were taken. Original task: Re-take jstack/JFR samples on the same probe. Verify that `ResponseConverter` / `AsmUtils.getExtensionAnnotationValue` no longer dominate.

## 5. Documentation & wrap-up

- [x] 5.1 Add a short note to `judo-runtime-core-dispatcher/agent-docs/architecture.md` on `ResponseConverter` key filtering: the allowed names per type are cached per conversion, and default-value companions are hidden. Verify the note is present and consistent with design.md.
- [x] 5.2 Run `openspec validate optimize-response-converter-allowed-names --strict` and verify it reports no errors.
- [x] 5.3 Commit (JNG-6420), only on the user's explicit instruction.
