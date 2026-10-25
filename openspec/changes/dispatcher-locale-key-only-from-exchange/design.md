## Context

Verified against `HEAD` of `feature/JNG-6415_MultiLanguageSupport`:

- **The shadowing site.** `judo-runtime-core-dispatcher/…/DefaultDispatcher.java`, lines 615–637:

  ```java
  if (exposed) {
      context.removeAll();                          // 620 — clears context on HTTP entry
  }
  try (MetricsCancelToken ignored = ...) {
      ...
      if (exchange.containsKey(ACTOR_KEY)) {
          context.putIfAbsent(ACTOR_KEY, exchange.get(ACTOR_KEY));  // 628–630 — "only-if-provided" pattern
      }

      Locale locale = defaultLocale;                                // 632 — JVM-default fallback
      if (exchange.containsKey(LOCALE_KEY)) {
          locale = (Locale) exchange.get(LOCALE_KEY);
      }
      context.putIfAbsent(LOCALE_KEY, locale);                      // 636 — ALWAYS writes
  ```

  `this.defaultLocale = Objects.requireNonNullElse(defaultLocale, Locale.getDefault())` at line 253
  guarantees the fallback is non-null; combined with `removeAll()` for exposed ops, the
  `putIfAbsent` at 636 is never a no-op on entry.

- **The consumer.** `PrincipalLocaleProvider.getLocale()`, anonymous branch (`PrincipalLocaleProvider.java:88-105`):

  1. `Context[LOCALE_KEY]` — return if non-null.
  2. `RequestLocaleHolder.getAcceptLanguage()` (subject to `LocaleResolutionLevel.BROWSER` ceiling)
     filtered through `PrincipalLocaleResolver.matchSupportedLanguage(...)`.
  3. `defaultLanguage`.

  Rule (1) is always satisfied through a dispatched op because of the write at line 636; rule (2)
  is unreachable through a dispatched anonymous op.

- **The `defaultLocale` builder parameter.** Optional Lombok `@Builder` parameter on
  `DefaultDispatcher` (line 223). A repo-wide grep across `/home/balazs/judo-ng` for
  `.defaultLocale(` on the `DefaultDispatcher` builder returns **zero hits** outside
  `DefaultDispatcher.java` itself. The three assemblers of `DefaultDispatcher`
  (`JudoDefaultSpringConfiguration.java:385`, `DefaultDispatcherProvider.java:133`,
  `DefaultDispatcherComponent.java:151`) all call `DefaultDispatcher.builder()…build()` with no
  locale set.

- **Other consumers of `Context[LOCALE_KEY]`.** Repo-wide grep finds one more:
  `ExportCall.java:78-79` uses
  `Objects.requireNonNullElseGet(context.getAs(Locale.class, LOCALE_KEY), () -> Locale.getDefault())`
  — already null-tolerant, will not regress.

- **The origin spec is aware, but incomplete.** The change note in
  `replace-browser-check-with-resolution-level/specs/anonymous-request-locale/spec.md`
  acknowledges `callOperation` "repopulates `LOCALE_KEY`/`ACTOR_KEY` from the exchange" — but the
  spec team never noticed that `LOCALE_KEY` is written **even when the exchange carries
  nothing**, using a JVM-default fallback. The spec's own "Header used when `LOCALE_KEY` is
  absent" scenario is currently unreachable through a dispatched op.

## Goals / Non-Goals

**Goals**
- Make the `add-anonymous-request-locale` + `replace-browser-check-with-resolution-level` browser
  tier reachable through `DefaultDispatcher.callOperation` — i.e. the existing spec scenarios
  hold end-to-end, not just in provider-unit-test isolation.
- Align `LOCALE_KEY` propagation with the `ACTOR_KEY` pattern already in place in the same
  method.
- Remove the now-orphan `defaultLocale` builder parameter and field.

**Non-Goals**
- No change to the anonymous-branch precedence rules themselves (still `LOCALE_KEY` → holder →
  default).
- No change to the authenticated principal-locale walk.
- No change to `RequestLocaleHolder` / `AcceptLanguageCaptureInterceptor` / Guice / Spring
  wiring.
- No compatibility shim on `DefaultDispatcher.builder()` (e.g. keeping `.defaultLocale(...)` as
  a `@Deprecated` no-op). Zero in-tree callers; the parameter's observable behaviour was the
  bug.
- No new consumer of `Context[LOCALE_KEY]`. `ExportCall`'s existing
  `Locale.getDefault()` fallback stays as-is (that call site is not dispatched through the
  provider chain and pre-dates JNG-6415).

## Decisions

### D1. Fix position — "application-set means truly application-set" vs "runtime always
resolves a `LOCALE_KEY`"

**Decision:** **Position A** — `LOCALE_KEY` in `Context` means "the exchange provided one",
period. When the exchange has no `LOCALE_KEY`, `Context[LOCALE_KEY]` remains absent, and the
`PrincipalLocaleProvider` precedence walk (`LOCALE_KEY` → holder → default) does the resolution.

**Rationale:**
- Matches the plain reading of `anonymous-request-locale/spec.md`'s precedence rule.
- Matches the sibling `ACTOR_KEY` handling three lines earlier.
- Requires the smallest patch (~5 lines) and no new API.
- Every consumer of `Context[LOCALE_KEY]` is already null-tolerant (`ExportCall` and
  `PrincipalLocaleProvider`), so removing the always-write is safe.

**Alternatives rejected:**

- **Position B — "`LOCALE_KEY` is the runtime's resolved locale."** Keep the always-write, and
  add a clause to `anonymous-request-locale/spec.md`: "except that the JVM-default-fallback
  `LOCALE_KEY` written by `DefaultDispatcher.callOperation` does not count as application-set."
  That clause is not expressible against the current `Context` API — there is no provenance bit
  distinguishing "app-set" from "dispatcher-defaulted". Making it expressible would require
  adding a marker key or a wrapper type, which is a larger API change than the fix itself.
  Rejected.

- **Position C — "Route the fallback through the provider instead."** Move
  `defaultLocale` into `PrincipalLocaleConfig` as a fourth tier below `defaultLanguage`. This
  duplicates `defaultLanguage`'s role (both are "what to return when nothing else resolved") and
  entangles a dispatcher wiring concern with a security-locale-config concern. Rejected.

### D2. Remove the `defaultLocale` builder parameter now, or leave it deprecated?

**Decision:** Remove immediately (field, `@Builder` param, JVM-default init).

**Rationale:**
- **Zero in-tree callers** across `judo-runtime-core`, `judo-runtime-core-esm-itest`, and
  `judo-platform` — verified by exhaustive grep in `/home/balazs/judo-ng` for
  `\.defaultLocale(` on paths that touch `DefaultDispatcher` (all three assemblers use bare
  `.builder()...build()`).
- The only behaviour the parameter delivered was the shadowing bug being fixed.
- `DefaultDispatcher.builder()` is an internal wiring surface used by three assemblers, not a
  documented app extension point. Deprecation buys nothing.

**Alternatives rejected:**
- Keep the field as `@Deprecated`, ignore it. Adds dead code, invites re-introduction of the
  bug via a well-meaning "wire this back up" change. Rejected.
- Rename the field (e.g. `fallbackLocale`) and route it through
  `PrincipalLocaleProvider`'s last tier. See D1 alternative C — rejected.

### D3. Test placement

**Decision:** Add a new unit-test class
`judo-runtime-core-dispatcher/src/test/java/…/dispatcher/DefaultDispatcherLocaleKeyTest.java`
covering the four scenarios locked into the specs (T1–T4 below).

**Rationale:**
- The existing `PrincipalLocaleProviderTest` cannot see the shadowing (it invokes the provider
  directly, without going through `callOperation`) — that is precisely why the bug survived
  JNG-6415.
- A DAO-integration ("itest") reproducer already exists in
  `judo-runtime-core-esm-itest:feature/JNG-6415_LocaleProviderInjectability`. That test's job is
  cross-repo integration proof; this repo's job is a fast unit-test guard against regression.
- `DefaultDispatcherTest`, where present, exists but its scaffolding is heavier than needed
  here. A slim standalone class with a minimal `DefaultDispatcher.builder()` fixture (real DAO,
  ASM, expression models pulled from an existing test resource, or Mockito for anything not
  under test) is preferred. **Open task** for the implementer: reuse if trivially reusable,
  otherwise handroll.

### D4. Task ordering — TDD

**Decision:** Follow the same TDD pattern used by `consolidate-locale-config-object`:
tests-first, watch red, then production edit, then green.

**Rationale:** Consistency with the repo's other JNG-6415 openspec changes.

## Risks / Trade-offs

- **Risk:** an out-of-tree `judo-*` deployment (not visible in this monorepo) called
  `DefaultDispatcher.builder().defaultLocale(...)`. **Mitigation:** the fix commit message and
  proposal `Impact` section call out the removal. The behaviour that consumer got — a
  JVM-default `LOCALE_KEY` on every dispatched op — was the bug; downstream consumers that
  wanted a hard-coded locale can set `Context[LOCALE_KEY]` in their filter, or set
  `PrincipalLocaleConfig.defaultLanguage`. Any grep in downstream repos will find the call
  quickly.
- **Trade-off:** removing the `defaultLocale` param slightly widens the change surface (from a
  ~5-line production edit to also touching the ctor). Chosen anyway because the alternative
  ("keep a dead parameter") is worse for future maintainers.

## Migration Plan

None required in-tree. Out-of-tree consumers (if any) drop `.defaultLocale(...)` from their
`DefaultDispatcher.builder()` chain; if they relied on the JVM-default `LOCALE_KEY` behaviour,
they should set `PrincipalLocaleConfig.defaultLanguage` instead (that value already survives the
`Context.removeAll()` in `callOperation` because `PrincipalLocaleConfig` is a wiring-time
singleton, not a per-request context entry).

## Open Questions

- **Should we also add an integration test in `judo-runtime-core-esm-itest`?** The reproducer
  branch there already documents the current buggy behaviour with a guard test that will flip
  once this change ships. Post-merge follow-up in that repo can rename the guard test's
  assertion and add a positive scenario. Not blocking this change.
