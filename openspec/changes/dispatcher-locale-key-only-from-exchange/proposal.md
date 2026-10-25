## Why

`DefaultDispatcher.callOperation` unconditionally populates `Context[LOCALE_KEY]` on entry —
falling back to `defaultLocale` (which itself defaults to `Locale.getDefault()`) when the exchange
carries no locale (`DefaultDispatcher.java:632-636`). Because
`PrincipalLocaleProvider.getLocale()`'s anonymous branch consults `Context[LOCALE_KEY]` first
(`PrincipalLocaleProvider.java:89`), that write **shadows the anonymous browser tier** the
`add-anonymous-request-locale` + `replace-browser-check-with-resolution-level` changes were
designed to expose.

Concretely, the spec scenario introduced by
`replace-browser-check-with-resolution-level/specs/anonymous-request-locale/spec.md`

> **Header used when `LOCALE_KEY` is absent** — anonymous, `LOCALE_KEY` absent, holder set →
> return the header locale

is **unreachable through a dispatched anonymous op**: after `DefaultDispatcher.callOperation`
runs `context.removeAll()` (for exposed HTTP-entry ops) and then `putIfAbsent(LOCALE_KEY,
defaultLocale)`, rule (1) of the anonymous precedence walk is always satisfied — with a value the
application never chose (the JVM default). `RequestLocaleHolder.getAcceptLanguage()` never runs.

Reproduced in `judo-runtime-core-esm-itest:feature/JNG-6415_LocaleProviderInjectability`,
`LocaleProviderInjectabilityTest#testBrowserCeilingReturnsHeaderLocale`: expected `hu-HU` (from
`Accept-Language`); observed `en` (from `Locale.getDefault()` on the test host). Detailed
write-up: `docs/JNG-6415-dispatcher-locale-key-shadowing.md`.

The existing `LOCALE_KEY` handling is also inconsistent with the `ACTOR_KEY` handling **two lines
above it in the same method** (`DefaultDispatcher.java:628-630`), which uses the correct
"propagate only when the exchange provides it" pattern.

## What Changes

- `DefaultDispatcher.callOperation` SHALL propagate `Context[LOCALE_KEY]` from the exchange
  **only when the exchange carries `LOCALE_KEY`** — mirroring the existing `ACTOR_KEY` treatment.
  When the exchange has no `LOCALE_KEY`, `Context[LOCALE_KEY]` SHALL remain absent, and
  `PrincipalLocaleProvider`'s own precedence walk (`LOCALE_KEY` → holder → `defaultLanguage`)
  becomes reachable end-to-end through a dispatched op.
- The `defaultLocale` builder parameter, field, and JVM-default fallback initialisation on
  `DefaultDispatcher` SHALL be removed. It has **zero callers** across the visible monorepo
  (`judo-runtime-core-spring`, `judo-runtime-core-guice`, `judo-platform-services` all build
  `DefaultDispatcher` without setting it) and, after the propagation fix, no code path reads it.
- No behaviour change for a dispatched op whose exchange **does** carry `LOCALE_KEY`: it continues
  to populate `Context[LOCALE_KEY]` with the same value.
- No behaviour change for the authenticated locale tier (principal payload / token attributes):
  that branch does not depend on `Context[LOCALE_KEY]`.
- Consumer-side null tolerance already exists:
  - `ExportCall` (`ExportCall.java:78-79`) already reads `Context[LOCALE_KEY]` with
    `Objects.requireNonNullElseGet(..., Locale::getDefault)`.
  - `PrincipalLocaleProvider` already handles the null case (falls through to holder →
    default-language).

## Impact

- **judo-runtime-core-dispatcher** — `DefaultDispatcher.callOperation` propagation block
  simplified (~5 lines net removed). `defaultLocale` field, ctor param, and JVM-default init line
  deleted. New unit tests locking in the behaviour.
- **judo-runtime-core-spring / judo-runtime-core-guice** — no source change (neither calls
  `.defaultLocale(...)` on the builder today).
- **Backwards compatibility** — the `DefaultDispatcher.builder()` API loses one optional
  parameter. Verified: no in-tree caller sets it (Spring config, Guice provider, judo-platform
  OSGi component). Any out-of-tree consumer that passed `.defaultLocale(...)` will fail to
  compile; behaviour they got (JVM-default `LOCALE_KEY` fallback) was the bug being fixed and
  had no correct use.
- **openspec/specs/dispatcher** — gains one **ADDED Requirement** locking the propagation rule.
- **openspec/specs/anonymous-request-locale** — gains one **ADDED Scenario** on the existing
  precedence requirement, proving the browser tier reaches through a dispatched op.
- **judo-runtime-core-esm-itest** cross-repo follow-up (informational, not blocking): the guard
  test `LocaleProviderInjectabilityTest#testBrowserCeilingReturnsHeaderLocale` flips from
  asserting `Locale.getDefault().toLanguageTag()` (documented as the buggy shadowed value) to
  asserting the header locale. That flip is the intended regression cross-check.
- **No metamodel, DAO, JAX-RS, or dispatcher-API surface change.**

## Capabilities

### New Capabilities
_None._

### Modified Capabilities
- `dispatcher` — gains one requirement: `DefaultDispatcher.callOperation` propagates
  `LOCALE_KEY` **only from the exchange**, never from a JVM-default fallback. Every other
  aspect of `callOperation` is unchanged.
- `anonymous-request-locale` — gains one scenario: the pre-existing "header used when
  `LOCALE_KEY` is absent" precedence rule is now reachable through
  `DefaultDispatcher.callOperation` with an empty exchange. Precedence order, tier ceiling
  semantics, and default fallback are unchanged.
