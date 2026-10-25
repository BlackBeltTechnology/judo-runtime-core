# JNG-6415: `DefaultDispatcher` shadows the anonymous browser-locale tier

**Status:** ✅ **Resolved &mdash; Position A landed in the origin repo during the same
session this finding was reported.** History preserved below for future reviewers.
Originally discovered while writing the ESM-itest for the "Backend operations may
inject `LocaleProvider`" requirement added by the
`replace-browser-check-with-resolution-level` change.

## Resolution timeline

- Reported here as a finding while writing `LocaleProviderInjectabilityTest` in
  `judo-runtime-core-esm-itest`.
- The origin agent adopted **Position A** (see "Recommended origin follow-up" below):
  `DefaultDispatcher.callOperation` no longer populates `Context[LOCALE_KEY]` from
  `Locale.getDefault()` when the exchange omits it. `PrincipalLocaleProvider`'s own
  default-language fallback covers the "no application choice" case.
- The itest carried two guard tests that intentionally asserted the pre-fix
  behaviour (JVM default winning through dispatch). Both flipped red during the
  full-suite regression run once the fix landed, exactly as designed. Their
  assertions were then flipped to lock in the post-fix behaviour — they are now
  canaries against future re-introduction of the shadowing.

Historical detail below.

**Reporter:** author of `judo-runtime-core-esm-itest` branch
`feature/JNG-6415_LocaleProviderInjectability`.

**Related:**
- Fix change (this repo): `openspec/changes/dispatcher-locale-key-only-from-exchange/` —
  proposal + design (Position A vs B vs C) + spec deltas for `dispatcher` and
  `anonymous-request-locale`.
- Fix commit: `332c8edd` (`JNG-6415: fix DefaultDispatcher LOCALE_KEY shadowing of anonymous browser tier`).
- Regression guard in this repo: `DefaultDispatcherLocaleKeyTest` (T1–T4).
- Parent origin change: `judo-runtime-core:openspec/changes/replace-browser-check-with-resolution-level/`
- Companion handoff: `docs/JNG-6415-task0-handoff-esm-itest.md`
- Reproducer: `judo-runtime-core-esm-itest` branch
  `feature/JNG-6415_LocaleProviderInjectability`,
  test class `hu.blackbelt.judo.runtime.core.dao.rdbms.locale.LocaleProviderInjectabilityTest`

## Summary

A backend operation dispatched via `DefaultDispatcher.callOperation` **cannot
observe the anonymous browser tier** of `PrincipalLocaleProvider.getLocale()`.
`callOperation` unconditionally populates `Context[LOCALE_KEY]` with either the
exchange-provided locale or, when the exchange has none, the JVM default
(`Locale.getDefault()`). The provider's own precedence rule then guarantees
that the freshly-populated `Context[LOCALE_KEY]` wins over
`RequestLocaleHolder.getAcceptLanguage()`, so the browser tier is unreachable
through a dispatched call — even when `localeResolutionLevel=BROWSER` and the
holder is set.

The origin `PrincipalLocaleProviderTest` unit tests cannot see this because
they invoke `getLocale()` directly, without going through `callOperation`.

## Evidence

**Dispatcher populates `LOCALE_KEY` unconditionally** —
`judo-runtime-core-dispatcher-1.0.6-SNAPSHOT-sources.jar`,
`DefaultDispatcher.java` lines 631–636 (also line 253 for the default
initialisation):

```java
// line 253 — defaultLocale falls back to JVM default when no explicit value is bound
this.defaultLocale = Objects.requireNonNullElse(defaultLocale, Locale.getDefault());

// line 620 — Context is cleared before each dispatched operation
context.removeAll();

// lines 631–636 — LOCALE_KEY is then always populated
Locale locale = defaultLocale;
if (exchange.containsKey(LOCALE_KEY)) {
    locale = (Locale) exchange.get(LOCALE_KEY);
}
context.putIfAbsent(LOCALE_KEY, locale);   // <-- Context now carries JVM default at minimum
```

Because `context.removeAll()` runs immediately before, the `putIfAbsent` is
never a no-op — it always writes `defaultLocale` when the exchange does not
carry `LOCALE_KEY`.

**Provider's anonymous branch reads `LOCALE_KEY` first** —
`judo-runtime-core-dispatcher-1.0.6-SNAPSHOT-sources.jar`,
`PrincipalLocaleProvider.java` (excerpt):

```java
// 3. Anonymous request: prefer an application-set request Locale...
if (context != null) {
    final Locale appLocale = context.getAs(Locale.class, DefaultDispatcher.LOCALE_KEY);
    if (appLocale != null) {
        return Optional.of(appLocale);        // <-- returns JVM default here, header never consulted
    }
}
// RequestLocaleHolder branch is below this — unreachable when LOCALE_KEY is populated
final String header = browserTierAllowed ? RequestLocaleHolder.getAcceptLanguage() : null;
```

## Reproducer (observed)

- Branch: `judo-runtime-core-esm-itest:feature/JNG-6415_LocaleProviderInjectability`
- Test:
  `hu.blackbelt.judo.runtime.core.dao.rdbms.locale.LocaleProviderInjectabilityTest#testBrowserCeilingReturnsHeaderLocale`
  (initial version — the pivoted final version documents the finding
  in-javadoc and asserts against the actual behaviour).
- Setup: fixture bound with `PrincipalLocaleConfig` where
  `localeResolutionLevel=BROWSER`,
  `supportedLanguages="en-US,hu-HU,de-DE"`, `defaultLanguage="en-US"`.
  `RequestLocaleHolder.set("hu-HU")` before dispatch. Exchange is empty.
- SDK function reads `injector.getInstance(LocaleProvider.class).getLocale()`.
- Expected (per origin spec's "Anonymous request with BROWSER ceiling reads
  the holder" scenario, transported through a real dispatched op): `hu-HU`.
- Observed: `en` (JVM default `Locale.ENGLISH.toLanguageTag()` on the test
  host — the value of `Locale.getDefault()` at fixture construction time).

## Interpretation

The origin `anonymous-request-locale/spec.md` locks in a precedence rule:

> 1. `Context[LOCALE_KEY]` (resolved `Locale`, if present) — respects an
>    explicit application choice.
> 2. `RequestLocaleHolder.getAcceptLanguage()` filtered through
>    `PrincipalLocaleResolver.matchSupportedLanguage(...)`.
> 3. `defaultLanguage`.

That rule is correct in isolation. The interaction with the dispatcher was
never spelled out: because `callOperation` writes `LOCALE_KEY` on entry (with
the JVM default as fallback), rule (1) is always satisfied inside a dispatched
operation body — with a value that the application never chose. The browser
tier becomes reachable only in code paths that call `getLocale()`
*outside* a dispatched operation (e.g. an `I18nServiceImpl.getMessage(...)`
invoked from a servlet filter or an error mapper), or from a dispatched
operation whose exchange carried an explicit `LOCALE_KEY`.

Whether this is a bug or a deliberate design depends on the intended use of
`LOCALE_KEY`. Two positions are internally consistent; the spec should pick
one and lock it in a test:

- **A. "Application-set" means truly application-set.** The dispatcher should
  not fabricate a `LOCALE_KEY` from `Locale.getDefault()` when the exchange
  omits it. Change to something like:
  ```java
  if (exchange.containsKey(LOCALE_KEY)) {
      context.putIfAbsent(LOCALE_KEY, exchange.get(LOCALE_KEY));
  }
  ```
  This makes the anonymous browser tier reachable through dispatch and
  matches the plain reading of the spec's precedence rule.

- **B. `LOCALE_KEY` is the runtime's resolved locale.** The dispatcher's
  behaviour is intentional — every operation should always see a non-null
  `Context[LOCALE_KEY]` — and the spec's anonymous precedence rule needs a
  clause: "except that the JVM-default-fallback `LOCALE_KEY` written by
  `DefaultDispatcher.callOperation` does not count as application-set." That
  clause is not expressible against the current `Context` API (there is no
  provenance bit), so this position practically forces adopting position A
  or adding a marker key.

Position A is smaller, matches operator intuition, and makes the origin
spec's own anonymous-browser-tier scenario reachable through a dispatched op.

## Options considered for the itest

The itest cannot fix this in the runtime; it can only choose how to
express the "backend op may inject `LocaleProvider`" proof:

1. **Direct SDK-function invocation.** Register the function via
   `getSdkFunctions().put(eop, fn)` (this proves the seam), but invoke it
   directly (`fn.apply(...)`) rather than through `callOperation`, with
   `Context.removeAll()` between assertions. Proves the injection contract
   and the ceiling propagation without depending on the dispatcher's
   `LOCALE_KEY` behaviour.
2. **Dispatch with an explicit exchange `LOCALE_KEY`.** Puts the itest in
   the class-3 (LOCALE_KEY-precedence) lane and stops proving the anonymous
   browser tier separately.
3. **Wait for the runtime fix (position A above).** Blocks the itest on the
   origin. Not chosen; the itest is unblocked with option 1 and a
   documentation cross-reference.

**Adopted:** option 1. `LocaleProviderInjectabilityTest` invokes the
registered SDK function directly for its ceiling assertions; one guard test
dispatches through `DefaultDispatcher.callOperation` and asserts what the
dispatcher actually returns today, with a javadoc pointing at this document
so the guard flags either behaviour change (a fix in origin, or a
regression).

## Recommended origin follow-up

- File a small change proposal (or fold into a follow-on JNG-6415 task)
  adopting **position A**: `DefaultDispatcher.callOperation` should only
  populate `Context[LOCALE_KEY]` when the exchange carries it, letting
  `PrincipalLocaleProvider`'s own default-language fallback (already the
  last step of `getLocale()`) handle the "no application choice" case.
- Add a scenario to `anonymous-request-locale/spec.md`:
  "**Anonymous request with BROWSER ceiling, dispatched through
  `DefaultDispatcher.callOperation` with an empty exchange, reads the
  holder**" — mirrors the existing non-dispatched scenario but locks in the
  post-fix behaviour.
- Once fixed, the itest's `testBrowserCeilingReturnsHeaderLocale` guard
  test in this repo will flip from asserting the JVM default to asserting
  the header, which is the correct regression cross-check.

## Not fixed here

This document only records the finding. No production code in either repo is
modified as part of the itest change that discovered it.
