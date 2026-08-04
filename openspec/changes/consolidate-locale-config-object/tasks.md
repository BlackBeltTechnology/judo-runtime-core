## 1. `PrincipalLocaleConfig` value type (TDD)

- [x] 1.1 Add `PrincipalLocaleConfigTest` in `judo-runtime-core-security/src/test/java/…/security/`
      covering: `isEnabled()` false when attribute null/blank, true otherwise;
      `getParsedSupportedLanguages()` returns the same set `PrincipalLocaleResolver.parseSupportedLanguages`
      would produce; parsed set is memoized (same instance across calls); `browserLanguageCheck`
      defaults to `Boolean.TRUE` via `@Builder.Default`; nulls tolerated on every field.
- [x] 1.2 Ran the test class — failed with compile errors as expected.
- [x] 1.3 Created `PrincipalLocaleConfig` in `judo-runtime-core-security/src/main/java/…/security/`
      as `@Value @Builder` with fields `principalLocaleAttribute`, `supportedLanguages`,
      `defaultLanguage`, `Boolean browserLanguageCheck` (`@Builder.Default = Boolean.TRUE`), a
      `@Getter(lazy = true) Set<String> parsedSupportedLanguages` delegating to
      `PrincipalLocaleResolver.parseSupportedLanguages(supportedLanguages)`, and `isEnabled()`.
- [x] 1.4 `mvn -pl judo-runtime-core-security test` is green.

## 2. `DefaultActorResolver` — collapse 4 builder fields into 1

- [x] 2.1 Updated both tests to build the SUT via `.localeConfig(...)`; also collapsed the pure `computeLocaleRefresh` signature (10 args → 7) to take a `PrincipalLocaleConfig`. and `DefaultActorResolverLocaleRefreshModelTest`
      to build the SUT via `.localeConfig(PrincipalLocaleConfig.builder()…build())`. Do not add or
      remove scenarios.
- [x] 2.2 Confirmed compile failures pre-implementation.
- [x] 2.3 In `DefaultActorResolver`: remove the four fields (`principalLocaleAttribute`,
      `supportedLanguages`, `defaultLanguage`, `browserLanguageCheck`); add a single
      `PrincipalLocaleConfig localeConfig` field; update the `@Builder` constructor accordingly;
      route `refreshActorLocale` and `computeLocaleRefresh` to read from `localeConfig`
      (`isEnabled()` for the feature gate, `getParsedSupportedLanguages()`, `getDefaultLanguage()`,
      `getBrowserLanguageCheck()`). Preserve the `Boolean → boolean` default-on semantics
      (`localeConfig == null || localeConfig.getBrowserLanguageCheck() == null ? true : …`).
- [x] 2.4 `mvn -pl judo-runtime-core-dispatcher test` green (dispatcher + locale refresh + model tests). — the locale-refresh
      scenarios from `JNG-6415` still pass.

## 3. `PrincipalLocaleProvider` — collapse 3 ctor args into 1

- [x] 3.1 Updated `PrincipalLocaleProviderTest` to pass a `PrincipalLocaleConfig`. to pass a `PrincipalLocaleConfig` to the ctor.
      Do not add or remove scenarios.
- [x] 3.2 Confirmed compile failure pre-implementation.
- [x] 3.3 Rewrote `PrincipalLocaleProvider` ctor to `(Context, PrincipalLocaleConfig)`; replace the
      three fields with the config; replace the per-instance `parseSupportedLanguages` call with
      `localeConfig.getParsedSupportedLanguages()`; keep every code path (principal → default →
      anonymous LOCALE_KEY → `RequestLocaleHolder` → default) byte-for-byte equivalent.
- [x] 3.4 `mvn -pl judo-runtime-core-dispatcher test` green.

## 4. Guice wiring

- [x] 4.1 In `DefaultActorResolverProvider`, replace the four qualified `@Inject`s with a single
      `@Inject(optional = true) @Nullable PrincipalLocaleConfig localeConfig`. Pass it into
      `DefaultActorResolver.builder().localeConfig(...)`.
- [x] 4.2 In `PrincipalLocaleProviderProvider`, replace the three qualified `@Inject`s with the
      same single injection; pass into `new PrincipalLocaleProvider(context, localeConfig)`. When
      the injection is absent, construct a default `PrincipalLocaleConfig.builder().build()` so the
      provider's feature-off path is preserved.
- [x] 4.3 In `JudoDefaultModule.configureConfiguration`, replace the four
      `bind(String/Boolean.class).annotatedWith(ActorResolver{…}).toInstance(...)` calls with a
      single `bind(PrincipalLocaleConfig.class).toInstance(PrincipalLocaleConfig.builder()… .build())`
      built from the four existing `JudoDefaultModuleConfiguration` getters. `browserLanguageCheck`
      defaults to `Boolean.TRUE` when the configuration value is null (matching current behaviour).
- [x] 4.4 Deleted the four now-unused `@BindingAnnotation` nested types (grep confirmed no other references outside stale `target/delombok` output). from
      `JudoConfigurationQualifiers`:
      `ActorResolverPrincipalLocaleAttribute`, `ActorResolverSupportedLanguages`,
      `ActorResolverDefaultLanguage`, `ActorResolverBrowserLanguageCheck`. Grep the tree to
      confirm no other reference.
- [x] 4.5 `mvn -pl judo-runtime-core-guice test` green.

## 5. Spring wiring

- [x] 5.1 Added `@Bean public PrincipalLocaleConfig getPrincipalLocaleConfig`(@Value…, …)` in
      `JudoDefaultSpringConfiguration` reading the four `judo.platform.*` properties (same defaults
      as today: attribute empty, supported empty, default empty, browserLanguageCheck `true`).
- [x] 5.2 Changed `getActorResolver`'s signature to take `PrincipalLocaleConfig` in place of the
      four locale `@Value`s; pass via `.localeConfig(...)`.
- [x] 5.3 Changed `getLocaleProvider`'s signature to take only `(PrincipalLocaleConfig)`; construct
      `new PrincipalLocaleProvider(context, localeConfig)`.
- [x] 5.4 `mvn -pl judo-runtime-core-spring test` green.

## 6. Full build + docs

- [x] 6.1 Full reactor `mvn clean install` green under Java 21, except `JudoDefaultPostgresqlModuleTest` in the two `-postgresql` modules which need Docker (TestContainers) — the same environmental skip already documented for JNG-6415. Re-ran `mvn install -pl '!judo-runtime-core-guice-postgresql,!judo-runtime-core-spring-postgresql'` → green.
- [x] 6.2 Updated `docs/JNG-6415-multi-language-support.md`: added `PrincipalLocaleConfig` row in § 5 and a new § 5.a "Configuration surface" clarifying the app-facing surface is unchanged.: add a short "Configuration surface"
      section noting that internally the four settings are grouped into `PrincipalLocaleConfig`;
      the four `judo.platform.*` property names and defaults are unchanged.
- [x] 6.3 `openspec validate consolidate-locale-config-object` → valid.
