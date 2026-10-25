## MODIFIED Requirements

### Requirement: Guice and Spring wiring

`judo-runtime-core-guice` and `judo-runtime-core-spring` SHALL expose the locale-configuration
bundle — `principalLocaleAttribute`, `supportedLanguages`, `defaultLanguage`,
`browserLanguageCheck` — as a **single immutable value object** (`PrincipalLocaleConfig`, in
`judo-runtime-core-security`), and SHALL inject that value object into both the login-time
locale refresher (`DefaultActorResolver`) and the request-aware `LocaleProvider`
(`PrincipalLocaleProvider`). The four platform properties / env vars
(`judo.platform.principalLocaleAttribute`, `supportedLanguages`, `defaultLanguage`,
`browserLanguageCheck`; corresponding `JUDO_PLATFORM_*` env vars), their defaults, and every
observable behaviour SHALL remain unchanged. Runtime-core SHALL NOT declare per-scalar Guice
`@BindingAnnotation` qualifiers for these four settings.

> Rationale: the four settings are one logical bundle consumed identically by two components;
> flowing them through the runtime as four independent scalar bindings creates duplication across
> Guice qualifiers, Guice module bindings, and Spring `@Value` sites (verified in JNG-6415 as one
> logical bundle expressed 3× and consumed 2×). Grouping them into a value object reduces the
> touch-surface for adding a fifth setting to ≤ 2 files.

#### Scenario: `PrincipalLocaleConfig` value object shape
- **WHEN** application code constructs `PrincipalLocaleConfig.builder().build()`
- **THEN** all four fields SHALL be present (`principalLocaleAttribute`, `supportedLanguages`,
  `defaultLanguage`, `browserLanguageCheck`), the object SHALL be immutable, `browserLanguageCheck`
  SHALL default to `true` when not supplied, and `getParsedSupportedLanguages()` SHALL return the
  same set that `PrincipalLocaleResolver.parseSupportedLanguages(supportedLanguages)` would return

#### Scenario: `isEnabled()` reflects the feature gate
- **WHEN** `principalLocaleAttribute` is null, empty, or whitespace-only
- **THEN** `PrincipalLocaleConfig.isEnabled()` SHALL return `false`, and every downstream consumer
  SHALL treat the feature as off (no header capture side-effect, no DB refresh, no locale override
  of backend messages)

#### Scenario: Parsed supported set is memoized
- **WHEN** `getParsedSupportedLanguages()` is called more than once on the same
  `PrincipalLocaleConfig` instance
- **THEN** the same `Set<String>` reference SHALL be returned (parse-once semantics)

#### Scenario: Guice wiring
- **WHEN** a Guice-based application is assembled and any of the four locale settings is present
  in `JudoDefaultModuleConfiguration`
- **THEN** a single `PrincipalLocaleConfig` SHALL be bound and injected into both
  `DefaultActorResolver` (via `.localeConfig(...)`) and `PrincipalLocaleProvider`; the four
  per-scalar `@BindingAnnotation` qualifiers introduced by `add-principal-locale-resolution`
  (`ActorResolverPrincipalLocaleAttribute`, `ActorResolverSupportedLanguages`,
  `ActorResolverDefaultLanguage`, `ActorResolverBrowserLanguageCheck`) SHALL NOT exist in
  `JudoConfigurationQualifiers`

#### Scenario: Spring wiring
- **WHEN** a Spring-based application sets any of `judo.platform.principalLocaleAttribute`,
  `supportedLanguages`, `defaultLanguage`, `browserLanguageCheck`
- **THEN** a single `PrincipalLocaleConfig` `@Bean` SHALL read all four properties and be injected
  into both the `ActorResolver` `@Bean` and the `LocaleProvider` `@Bean`; neither bean SHALL take
  the individual `@Value("${judo.platform.…}")` parameters for these four settings

#### Scenario: Behaviour parity with JNG-6415
- **WHEN** any scenario from `add-principal-locale-resolution` (precedence tier walk, refresh
  on-change, feature-off gate, non-mapped-attribute WARN, refresh failure WARN, backend i18n
  message locale) or `add-anonymous-request-locale` (anonymous `Accept-Language`, thread-local
  holder capture and clear, application-set `LOCALE_KEY`, authenticated principal wins) is
  executed against the refactored wiring
- **THEN** the observable outcome SHALL be identical to the pre-refactor behaviour
