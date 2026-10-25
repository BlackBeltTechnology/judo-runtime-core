## 0. Verify `LocaleProvider` is reachable from a custom backend operation

**Decision:** the access path is an injected `LocaleProvider` / `I18nService` — not a JUDO
expression / `Context` variable, no `LocaleVariableProvider`. Custom operations are supplied as
`Function<Payload,Payload>` via `DispatcherFunctionProvider.getSdkFunctions()` and invoked by
`DefaultDispatcher.callOperation`. They are created in the same DI injector as the runtime
bindings, so an operation impl can `@Inject LocaleProvider` directly.

- [x] **0.1** Confirmed. Neither `JudoDefaultModule.configureLocaleProvider()` (Guice) nor
  `JudoDefaultSpringConfiguration.getLocaleProvider(...)` (Spring) changed in this refactor. The
  full-injector `JudoDefaultHsqldbModuleTest` still passes, so the singleton binding round-trips
  cleanly with the new `LocaleResolutionLevel` enum threaded through `PrincipalLocaleConfig`.
- [ ] **0.2** Add an integration test proving a backend `Function<Payload,Payload>` operation can
  `@Inject LocaleProvider` and that, inside an authenticated operation call, `getLocale()` returns
  the principal's resolved locale — asserting it changes with the configured
  `LocaleResolutionLevel` (assemble the injector three times with `BROWSER`,
  `IDENTITY_PROVIDER`, `PRINCIPAL` and observe the return flip across the three cases).
  **Host module:** `judo-runtime-core-guice-hsqldb` (extends the existing
  `JudoDefaultHsqldbModuleTest` — full injector already wired, no fixture ceremony needed) unless
  the reviewer prefers `judo-runtime-core-guice-testkit`.
- [ ] **0.3** Include an explicit assertion that the resolved locale is correct **without** the
  deferred DB write-back path — the in-request actor payload is either mutated by
  `refreshActorLocale` (Option A footing) or read straight off `Context[ACTOR_KEY]`. Documenting
  this here so a future re-enable of `dao.update` does not silently change the tested contract.
- [ ] **0.4** **App-level (document, do not build in this repo):** note in the PR description
  that the frontend-reachable operation is an *exposed/unbound* operation in the application model
  whose custom impl `@Inject`s `LocaleProvider` / `I18nService` and returns / uses the locale.
  Runtime-core only guarantees injectability; the demonstrator operation lives in the generated
  app / a sample project.

## 1. New enum in `judo-runtime-core-security`

- [x] **1.1** Created (constants in ascending precedence order: `PRINCIPAL`, `IDENTITY_PROVIDER`,
  `BROWSER`).
- [x] **1.2** `DEFAULT = BROWSER` added.
- [x] **1.3** `includes(tier)` returns `ordinal() >= tier.ordinal()`; null argument throws NPE.
- [x] **1.4** `parse(String)` — lenient, case-insensitive, trimmed; null / blank / unknown →
  `DEFAULT`; never throws.
- [x] **1.5** EPL header included.
- [x] **1.6** `LocaleResolutionLevelTest` — 10 tests across two nested groups (`Includes` × 3,
  `Parse` × 7). Green.

## 2. Update the pure resolver

- [x] **2.1** Last param swapped to `LocaleResolutionLevel level`.
- [x] **2.2** Tier walk gated by ceiling; `null` level treated as `DEFAULT` (`BROWSER`).
- [x] **2.3** `matchAgainstSupported`, `parseSupportedLanguages`, `matchSupportedLanguage`,
  `ACCEPT_LANGUAGE_ATTRIBUTE`, `FALLBACK_DEFAULT` unchanged.
- [x] **2.4** Class-header, `@param acceptLanguageHeader`, and `@param level` javadoc updated.
- [x] **2.5** All 27 call sites mechanically substituted via `sed`.
- [x] **2.6** Nested `ResolutionLevelCeiling` group added — 6 tests.

## 3. Update the config value object

- [x] **3.1** Field swapped; javadoc updated to describe the enum ceiling; `isEnabled()`,
  `@Getter(lazy = true) parsedSupportedLanguages` memoization, and every other field preserved.
- [x] **3.2** All builder calls and getters swapped; default-builder test asserts
  `localeResolutionLevel=BROWSER`; a second test locks in that explicit `PRINCIPAL` /
  `IDENTITY_PROVIDER` values are preserved through the builder. equals/hashCode + memoization
  tests preserved.

## 4. Update the login-refresh consumer

- [x] **4.1** Boolean-derivation block deleted; `config.getLocaleResolutionLevel()` passed to
  `PrincipalLocaleResolver.resolve(...)`. Import added. `localeConfig` field javadoc updated.
  Persistence path (commented `dao.update`) left as-is.
- [x] **4.2** `cfg(attr, browserCheck)` maps the boolean to `BROWSER` / `IDENTITY_PROVIDER`.
  Model test uses `.localeResolutionLevel(LocaleResolutionLevel.BROWSER)` with explicit import.
  Both test classes green.

## 5. Update the per-request read consumer

- [x] **5.1** Anonymous branch derives `browserTierAllowed = level == null ||
  level.includes(BROWSER)` before touching `RequestLocaleHolder`. Import added.
- [x] **5.2** Three new tests: `anonymousBrowserTierOnAtBrowserCeilingResolvesHeader`,
  `anonymousBrowserTierSkippedAtIdentityProviderCeiling`,
  `anonymousBrowserTierSkippedAtPrincipalCeiling`. Provider test grew 15 → 18.

## 6. Update the Keycloak login interceptor

- [x] **6.1** Builder param renamed; private field `captureBrowserLanguage` derived once from the
  level (null-safe). Static helper's boolean signature preserved per D6.
- [x] **6.2** `KeycloakLoginInterceptorAcceptLanguageTest` compiles + passes unchanged (6/6).

## 7. 🔴 Fix the Guice–Keycloak wiring gap

- [x] **7.1** `PrincipalLocaleConfig` injected `optional = true`; level threaded into the
  builder. `judo-runtime-core-security` already an explicit dependency, no dep-graph edit needed.
  Fixes the wiring gap.
- [x] **7.2** New `KeycloakLoginInterceptorProviderLocaleTest` — 4 reflection-based tests
  covering `PRINCIPAL`, `IDENTITY_PROVIDER`, `BROWSER`, and null-config default. Green.

## 8. Guice wiring

- [x] **8.1** Field renamed to `actorResolverLocaleResolutionLevel`; default `BROWSER`; import
  added.
- [x] **8.2** Builder param renamed and threaded through the fallback builder block. Binding uses
  `.localeResolutionLevel(configuration.getActorResolverLocaleResolutionLevel())` — the enum's
  `@Builder.Default` collapses the previous null-check. Import added; comment updated.
- [x] **8.3** `JudoConfigurationQualifiers` comment refreshed to describe the four settings
  including the enum ceiling.

## 9. Spring wiring

- [x] **9.1** `@Value("${judo.platform.localeResolutionLevel:BROWSER}") String` feeds
  `LocaleResolutionLevel.parse(...)` into the config builder. Import satisfied by the existing
  wildcard `hu.blackbelt.judo.runtime.core.security.*`.
- [x] **9.2** Confirmed. `getActorResolver(...)` / `getLocaleProvider(...)` take the
  `PrincipalLocaleConfig` bean unchanged. Spring interceptor registration deferred to the starter.

## 10. Docs & spec reconciliation

- [x] **10.1** Env-var row swapped, precedence-table footnote updated,
  `KeycloakLoginInterceptor` row rewritten, new `§ 4.a` explains the ceiling with the three-row
  truth table, new `§ 6.a` documents the two latent gaps fixed.
- [x] **10.2** Change docs (proposal / design / tasks + two spec deltas) validate strict.
- [x] **10.3** Verified: no historical change docs rewritten. Earlier ad-hoc edits to
  `add-principal-locale-resolution/{proposal,design}.md` were reverted before folding into this
  change's spec deltas.

### 10.a Reconcile the living reference doc with the implemented reality

A post-implementation review of the JNG-6415 branch surfaced four wording issues in
`docs/JNG-6415-multi-language-support.md` that no longer match the code. All four are corrected
here (living doc — not a change proposal, safe to edit); the analogous requirement-level
corrections are captured as MODIFIED requirements in this change's spec deltas (see
`specs/principal-locale-resolution/spec.md` D9 and `specs/anonymous-request-locale/spec.md` D10).

- [x] **10.a.1** §1 rewritten to scope the `principalLocaleAttribute` gate to authenticated
  paths; explicit note that anonymous request locale resolves independently.
- [x] **10.a.2** §6 bullet rewritten: documents write suppression + single WARN; states no
  default is forced; actor payload's stored value (including `null`) preserved.
- [x] **10.a.3** §9 test-count bullet updated to "18 `@Test` methods total (8 original + 7
  anonymous + 3 anonymous-ceiling)".
- [x] **10.a.4** §5 `PrincipalLocaleResolver` row rewritten to enumerate the three helpers +
  the constant. New row added for `LocaleResolutionLevel`. `KeycloakLoginInterceptor` row
  updated (enum builder param + derived boolean).
- [x] **10.a.5** §7 verified against current code — accurate as-is. Extended with a one-line
  note on the "backend reads only" review decision.
- [ ] **10.a.6** Note in the PR description that the parent
  `add-principal-locale-resolution/proposal.md` line 90's "No interface or signature changes"
  wording is technically inaccurate (`DefaultActorResolver.builder()` and
  `PrincipalLocaleProvider`'s ctor did grow), but the historical proposal remains as-drafted per
  the OpenSpec convention that in-flight change docs describe their proposed state; the correct
  scoping (public JUDO interfaces — `DAO`, `Dispatcher`, `LocaleProvider` — unchanged; internal
  builder/ctor extended) is captured in D12 of this change's design.

## 11. Verify

- [x] **11.1** Reactor `mvn install` green for all 30 non-parent modules; only
  `judo-runtime-core-guice-postgresql` fails due to the pre-existing Docker-unavailable
  TestContainers issue. Downstream SKIPPED modules built via a separate `mvn install -pl
  '!judo-runtime-core-guice-postgresql'` run — all SUCCESS.
- [x] **11.2** Grep clean of live references. Surviving occurrences are intentional historical
  markers — the enum's "Replaces the boolean" javadoc, the config's "Replaces the boolean"
  javadoc, test `@DisplayName`s that mark the historical `true/false` mapping, and the static
  `captureAcceptLanguage(..., boolean)` helper whose signature is preserved per D6.
- [x] **11.3** Verified: all three levels reachable via `judo.platform.localeResolutionLevel`
  (Spring, `LocaleResolutionLevel.parse` lenient), `actorResolverLocaleResolutionLevel` (Guice
  `JudoDefaultModuleConfiguration`), and — pending the platform wiring follow-up —
  `JUDO_PLATFORM_LOCALE_RESOLUTION_LEVEL` (env var). Default `BROWSER`.

## 12. Merge gate

- [ ] **12.1** Push the branch and open the PR — triggers the **CodeRabbit** review (repo-level PR
  gate, runs after push not locally). If CodeRabbit is not configured for this repo, either
  enable the CodeRabbit GitHub App or add a minimal `.coderabbit.yaml`; confirm with repo admins
  first.
- [ ] **12.2** Address every finding (or resolve/dismiss with justification). Treat unresolved 🔴
  findings — especially the Task 7 Guice gap — as merge-blocking.
- [ ] **12.3** Merge to `develop` only once the rabbit review is green **and** `build.yml` CI
  passes.
