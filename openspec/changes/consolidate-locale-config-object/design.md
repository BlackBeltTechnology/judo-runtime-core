## Context

Verified against the tree at HEAD of `feature/JNG-6415_MultiLanguageSupport`:

- `DefaultActorResolver` (`judo-runtime-core-dispatcher/…/DefaultActorResolver.java`) has a `@Builder`
  ctor accepting 10 fields, of which 4 are locale configuration
  (`principalLocaleAttribute`, `supportedLanguages`, `defaultLanguage`, `browserLanguageCheck`).
- `PrincipalLocaleProvider` (`…/dispatcher/environment/PrincipalLocaleProvider.java`) takes 4 ctor
  args, of which 3 are locale configuration; it also calls
  `PrincipalLocaleResolver.parseSupportedLanguages(supportedLanguages)` in the ctor — once per
  provider instance.
- `JudoConfigurationQualifiers` (`judo-runtime-core-guice/…/JudoConfigurationQualifiers.java`) declares
  4 dedicated `@BindingAnnotation` types for these four values.
- `DefaultActorResolverProvider` and `PrincipalLocaleProviderProvider` each `@Inject(optional=true)`
  the qualified scalars — 4 and 3 injections respectively.
- `JudoDefaultModule.configureConfiguration` performs 4 conditional
  `bind(...).annotatedWith(...).toInstance(...)` calls, each guarded by a null check.
- `JudoDefaultSpringConfiguration.getActorResolver` and `getLocaleProvider` each redeclare the same
  three `@Value("${judo.platform.…}")` params (with a fourth for the resolver).

The four settings are one **logical bundle**: they are always consumed together by both the
login-time refresher (`DefaultActorResolver`) and the per-request reader (`PrincipalLocaleProvider`),
they are all sourced from the same `JUDO_PLATFORM_*` env-var family, and their semantics are defined
jointly by the `principal-locale-resolution` spec.

## Goals / Non-Goals

**Goals**
- Represent the locale-configuration bundle as one immutable value object flowing through the
  runtime.
- Reduce wiring code (Guice qualifiers, Guice module, Spring config) so adding a fifth locale
  setting in the future touches ≤ 2 files (the value type + `JudoDefaultModuleConfiguration`).
- Parse `supportedLanguages` (CSV → `Set<String>`) exactly once per wiring, not once per
  `PrincipalLocaleProvider` construction.
- Keep every observable behaviour, precedence tier, default, and property name identical to
  `JNG-6415`.

**Non-Goals**
- No new locale semantics. No new tiers. No behavioural change under any input.
- No compatibility shim on `DefaultActorResolver.builder()` (e.g. keeping the four scalar setters
  as `@Deprecated` aliases). The builder is an internal wiring surface, exercised in tree only by
  the two providers and the JNG-6415 tests.
- No renaming of `judo.platform.*` properties, `JUDO_PLATFORM_*` env vars, or
  `JudoDefaultModuleConfiguration` fields. App-facing surface is preserved.
- No consolidation with `checkMappedActors` / `acceptableClients` — those are unrelated
  ActorResolver concerns.

## Decisions

### D1. Where does `PrincipalLocaleConfig` live?

**Decision:** `judo-runtime-core-security`, alongside `PrincipalLocaleResolver`.

**Rationale:** `PrincipalLocaleResolver` is the pure helper this bundle exists to feed. The
resolver already owns `parseSupportedLanguages` and `matchSupportedLanguage`. Co-locating the value
object keeps the "pure logic" module the single source of truth for locale-config data + parsing.
`judo-runtime-core-dispatcher` and every Guice/Spring wiring module already depend on
`judo-runtime-core-security`, so no new dependency edge is added.

**Alternatives rejected:**
- `judo-runtime-core` (the "core abstractions" module) — too generic; would require pulling
  `PrincipalLocaleResolver`'s parsing there too, which is out of scope.
- Nested static class inside `DefaultActorResolver` — hides it from `PrincipalLocaleProvider`
  (in a different module) and from Guice/Spring wiring.

### D2. What shape — Lombok `@Value @Builder` vs Java `record`?

**Decision:** Lombok `@Value @Builder`.

**Rationale:** The rest of the runtime-core codebase uses Lombok extensively (the parent guide
lists Lombok 1.18.38 as the code-generation tool; `DefaultActorResolver` itself is `@Builder`).
Consistency wins. `@Value` provides null-tolerant getters and immutability; `@Builder.Default`
handles the `browserLanguageCheck = true` default; `@Getter(lazy = true)` memoizes the parsed
`Set<String>` at first read — a feature not available on `record`.

**Alternatives rejected:**
- Java `record` — no easy memoization of the parsed set without hand-rolling a companion class;
  breaks stylistic consistency.
- Plain POJO with explicit ctor + fields — verbose, no value semantics.

### D3. Should the four Guice qualifier annotations be kept for back-compat?

**Decision:** Delete them.

**Rationale:** They were introduced by this same feature branch (JNG-6415) and have no consumers
outside the two providers this change edits (verified via grep). Keeping them would leave a
misleading extension point — a downstream user could `@Named`-bind
`@ActorResolverPrincipalLocaleAttribute String` and observe nothing happen because
`DefaultActorResolverProvider` no longer reads it. Deleting them removes the trap.

**Alternatives rejected:**
- Keep them as `@Deprecated` and read them in `JudoDefaultModule` to build the
  `PrincipalLocaleConfig` — reintroduces exactly the 3× duplication the refactor removes.

### D4. `PrincipalLocaleProvider` with a null config

**Decision:** In `PrincipalLocaleProviderProvider`, when `PrincipalLocaleConfig` is not bound
(nothing configured the feature), construct a `PrincipalLocaleConfig.builder().build()` default —
all fields null except `browserLanguageCheck = true`. The provider's existing feature-off paths
already handle blank/null `principalLocaleAttribute` and empty `parsedSupportedLanguages`.

**Rationale:** Avoids a nullable `PrincipalLocaleConfig` field inside the provider. The empty
default is semantically indistinguishable from "feature not configured" (which is exactly what an
absent injection means today).

**Alternatives rejected:**
- Make the provider's field `@Nullable` and null-check on every call — more branches, same result.
- Bind a default `PrincipalLocaleConfig` unconditionally in `JudoDefaultModule` — considered, but
  the current pattern uses `bind(...).toInstance(configuration.get…)` guarded by null; the
  simplest fix is a builder default in the provider itself so an incompletely configured
  standalone `Injector` still works.

### D5. Test migration policy

**Decision:** Update SUT construction only. Do **not** add scenarios, remove scenarios, or change
assertions. Every scenario from `add-principal-locale-resolution` and `add-anonymous-request-locale`
continues to hold post-refactor.

**Rationale:** This is a mechanical wiring refactor. The pure precedence/tier logic lives in
`PrincipalLocaleResolver`, which is untouched. Test parity is the correctness contract.

## Risks / Trade-offs

- **Internal breakage risk:** any downstream code calling `DefaultActorResolver.builder()` or
  `new PrincipalLocaleProvider(...)` directly must migrate. Both are internal wiring types (no
  Javadoc contract, no `@API` annotation), and neither is documented as an extension point. Blast
  radius expected: this repo's tests + the two in-tree providers.
- **Guice qualifier deletion:** external users who copied the pattern would break at compile time.
  Given the qualifier annotations shipped in JNG-6415 (same release), this is a same-release
  correction rather than a regression against a prior stable release.
- **Memoization semantics:** `@Getter(lazy = true)` is thread-safe (double-checked lock generated
  by Lombok). Since `PrincipalLocaleConfig` is a singleton in both Guice and Spring, the parsed
  set is computed at most once per JVM.

## Migration Plan

1. Land `PrincipalLocaleConfig` + tests (task 1). No consumers yet; additive.
2. Land the two consumer refactors in the **same commit** as the wiring changes (tasks 2–5), so
   the tree is never in a state where the value object exists but is unwired, and the four
   qualifiers exist without consumers. This keeps `mvn clean install` green at every commit.
3. Full reactor build + docs update (task 6).

No data migration. No env-var / property renames. No feature-flag needed.

## Open Questions

_None._ The scope is fixed by the four settings introduced in JNG-6415.
