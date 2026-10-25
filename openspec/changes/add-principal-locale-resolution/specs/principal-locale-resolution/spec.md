## ADDED Requirements

### Requirement: Platform parameters for principal locale resolution

The system SHALL support four platform parameters that configure principal locale resolution. Each parameter SHALL be settable via a `JUDO_PLATFORM_*` environment variable (mapped by the follow-up platform change) or via the corresponding Guice / Spring configuration property in this change.

| Env var | Property | Type | Default |
|---|---|---|---|
| `JUDO_PLATFORM_PRINCIPAL_LOCALE_ATTRIBUTE` | `principalLocaleAttribute` | String | unset ⇒ feature OFF |
| `JUDO_PLATFORM_SUPPORTED_LANGUAGES` | `supportedLanguages` | comma-separated BCP-47 list | `{defaultLanguage}` |
| `JUDO_PLATFORM_DEFAULT_LANGUAGE` | `defaultLanguage` | BCP-47 tag | JVM default / `en-US` |
| `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK` | `browserLanguageCheck` | boolean | `true` |

#### Scenario: All parameters unset
- **WHEN** none of the four parameters are configured
- **THEN** the runtime SHALL behave identically to the pre-change baseline: no `Accept-Language` capture, no DB refresh, and the `LocaleProvider` reference stays unbound

#### Scenario: Only `principalLocaleAttribute` set
- **WHEN** `principalLocaleAttribute=locale` and the other three parameters are unset
- **THEN** `supportedLanguages` SHALL default to `{defaultLanguage}`, `defaultLanguage` SHALL default to the JVM default (or `en-US` if the JVM default is not a valid BCP-47 tag), and `browserLanguageCheck` SHALL default to `true`

### Requirement: Feature gate on `principalLocaleAttribute`

The principal locale refresh, `Accept-Language` capture, and any `LocaleProvider` effect SHALL be disabled entirely when `principalLocaleAttribute` is null, empty, or whitespace-only.

#### Scenario: Feature disabled — no capture, no refresh
- **WHEN** `principalLocaleAttribute` is unset
- **AND** an authenticated request arrives with an OIDC `locale` claim and `Accept-Language: hu-HU`
- **THEN** `KeycloakLoginInterceptor` SHALL NOT read the `Accept-Language` header
- **AND** `DefaultActorResolver.getActorByClaims` SHALL NOT call `DAO.update` for a locale attribute
- **AND** `PrincipalLocaleProvider.getLocale()` SHALL return the default (identical to unbound `LocaleProvider`)

### Requirement: Fixed precedence order

The `PrincipalLocaleResolver` helper SHALL walk candidate locales in a fixed order: `browser` → `claim` → `stored` → `default`. Each tier's candidate SHALL be validated against `supportedLanguages` via RFC-4647 lookup; a candidate that does not match SHALL be skipped so the next tier is consulted. The `default` tier SHALL always resolve.

#### Scenario: Browser tier wins when enabled and supported
- **WHEN** `browserLanguageCheck=true`, `supportedLanguages={en-US, hu-HU}`, `Accept-Language: hu`, claim locale `en-US`, stored locale `en-US`
- **THEN** the resolver SHALL return `hu-HU` (RFC-4647 lookup: `hu` → `hu-HU`)

#### Scenario: Claim tier wins when browser disabled
- **WHEN** `browserLanguageCheck=false`, `supportedLanguages={en-US, hu-HU}`, `Accept-Language: hu` (ignored), claim locale `hu-HU`, stored locale `en-US`
- **THEN** the resolver SHALL return `hu-HU` from the claim; the `Accept-Language` header SHALL NOT be consulted

#### Scenario: Unsupported browser tag falls through
- **WHEN** `browserLanguageCheck=true`, `supportedLanguages={en-US}`, `Accept-Language: hu-HU`, claim locale `en-US`, stored locale null
- **THEN** the resolver SHALL skip the browser tier and return `en-US` from the claim tier

#### Scenario: Stored tier wins when browser and claim empty or unsupported
- **WHEN** `browserLanguageCheck=false`, `supportedLanguages={en-US, hu-HU}`, claim locale null, stored locale `hu-HU`
- **THEN** the resolver SHALL return `hu-HU` from the stored tier

#### Scenario: Default terminal fallback
- **WHEN** `supportedLanguages={en-US, hu-HU}`, `defaultLanguage=en-US`, all higher tiers empty or unsupported
- **THEN** the resolver SHALL return `en-US`

### Requirement: Supported-language filtering (RFC-4647 lookup)

Every candidate locale from every tier SHALL be looked up against `supportedLanguages` using RFC-4647 lookup semantics (language range → language tag). A candidate that does not resolve to any supported tag SHALL be treated as absent for that tier.

#### Scenario: Language-only range matches region-tagged supported tag
- **WHEN** `supportedLanguages={hu-HU, en-US}` and the candidate is `hu`
- **THEN** the lookup SHALL return `hu-HU`

#### Scenario: Region mismatch skips candidate
- **WHEN** `supportedLanguages={en-US}` and the candidate is `en-GB`
- **THEN** the lookup SHALL NOT match `en-US` (RFC-4647 lookup narrows, never widens) and the candidate SHALL be skipped

#### Scenario: Malformed tag skipped
- **WHEN** the candidate is a malformed tag (empty, contains whitespace, or fails `Locale.LanguageRange` parsing)
- **THEN** the resolver SHALL skip the candidate without throwing

### Requirement: Browser tier gated by `browserLanguageCheck`

When `browserLanguageCheck=true` (the default), the `Accept-Language` header SHALL be read by `KeycloakLoginInterceptor`, stashed into `JudoPrincipal.attributes["__acceptLanguage"]`, and consumed by the resolver's browser tier as the top precedence source. When `browserLanguageCheck=false`, the header SHALL NOT be read and the browser tier SHALL be omitted from the precedence walk.

#### Scenario: Gate off — header not captured
- **WHEN** `browserLanguageCheck=false` and a request arrives with `Accept-Language: hu-HU`
- **THEN** `JudoPrincipal.attributes` SHALL NOT contain the key `__acceptLanguage`

#### Scenario: Gate on — header captured
- **WHEN** `browserLanguageCheck=true` and a request arrives with `Accept-Language: hu-HU,en-US;q=0.7`
- **THEN** `JudoPrincipal.attributes["__acceptLanguage"]` SHALL be set to the raw header value
- **AND** the resolver SHALL parse quality-ordered ranges and consult them against `supportedLanguages` in order

#### Scenario: Gate on but header absent
- **WHEN** `browserLanguageCheck=true` and the request has no `Accept-Language` header
- **THEN** the browser tier SHALL be empty and the resolver SHALL fall through to the claim tier

### Requirement: Login-time refresh persists resolved locale

`DefaultActorResolver.getActorByClaims` SHALL, after the actor entity is loaded and before returning, invoke `PrincipalLocaleResolver` with the browser hint (from `__acceptLanguage`), the claim locale (from claims), the stored locale (from the loaded payload), the default, the supported set, and the browser gate. If the resolved value differs from the stored value, the resolver SHALL call `dao.update(actorType, payloadWithId+localeAttr, null)`. Feature-gated on non-blank `principalLocaleAttribute`.

#### Scenario: Resolved differs from stored — update called
- **WHEN** `principalLocaleAttribute=locale`, resolved locale is `hu-HU`, stored locale is `en-US`
- **THEN** `dao.update` SHALL be called with a payload containing the actor identifier and `{locale: "hu-HU"}`

#### Scenario: Resolved equals stored — no update
- **WHEN** resolved locale is `hu-HU` and stored locale is `hu-HU`
- **THEN** `dao.update` SHALL NOT be called

#### Scenario: DAO update throws — auth continues
- **WHEN** `dao.update` throws an exception
- **THEN** the exception SHALL be caught, a WARN SHALL be logged with the actor id, and `getActorByClaims` SHALL return the actor payload as if the refresh had not been attempted

#### Scenario: `principalLocaleAttribute` unset — refresh skipped
- **WHEN** `principalLocaleAttribute` is null or blank
- **THEN** `getActorByClaims` SHALL NOT invoke `PrincipalLocaleResolver` and SHALL NOT call `dao.update` for a locale

### Requirement: `principalLocaleAttribute` must be a mapped attribute

If `principalLocaleAttribute` is set but does not resolve to a mapped attribute on the actor `EClass`, the refresh SHALL be skipped and a one-time WARN SHALL be logged naming the offending attribute. Authentication SHALL NOT fail.

#### Scenario: Attribute not present on actor EClass
- **WHEN** `principalLocaleAttribute=locale` but the actor `EClass` has no attribute named `locale`
- **THEN** no `dao.update` call SHALL be made
- **AND** a WARN SHALL be logged once (per JVM lifetime, per actor type)
- **AND** the request SHALL proceed with the resolved locale determined via the fallback chain

#### Scenario: Attribute exists but is transient
- **WHEN** `principalLocaleAttribute=locale` and the actor has a `locale` attribute marked `transient`
- **THEN** no `dao.update` call SHALL be made
- **AND** a WARN SHALL be logged once

### Requirement: `Accept-Language` capture in KeycloakLoginInterceptor

When `browserLanguageCheck=true`, `KeycloakLoginInterceptor.handleMessage` SHALL read the `Accept-Language` header from the `HttpServletRequest` and place its raw value into the principal attributes under the key `__acceptLanguage` before constructing the `JudoPrincipal`. The key SHALL use a double-underscore prefix to signal a runtime-internal attribute distinct from any Keycloak claim.

#### Scenario: Header present
- **WHEN** the request has `Accept-Language: hu-HU,en-US;q=0.7` and the gate is on
- **THEN** `attributes["__acceptLanguage"]` SHALL equal the string `hu-HU,en-US;q=0.7`

#### Scenario: Header absent
- **WHEN** the request has no `Accept-Language` header and the gate is on
- **THEN** `attributes["__acceptLanguage"]` SHALL be absent (not set to null, not set to empty string)

#### Scenario: Non-Keycloak auth path
- **WHEN** authentication happens through a non-Keycloak interceptor
- **THEN** no `Accept-Language` capture is guaranteed; the browser tier SHALL be treated as empty and the resolver SHALL fall through to the claim tier

### Requirement: Backend messages follow the resolved locale via `PrincipalLocaleProvider`

The `judo-runtime-core-dispatcher` module SHALL provide a class `PrincipalLocaleProvider` implementing `hu.blackbelt.osgi.i18n.api.LocaleProvider`. Its `getLocale()` method SHALL return a `java.util.Locale` parsed from the principal's resolved locale attribute (read via `PrincipalVariableProvider.apply(principalLocaleAttribute)`). When no principal is bound or the attribute is unset, it SHALL return the configured `defaultLanguage` as a `Locale`.

#### Scenario: Principal has a resolved locale
- **WHEN** a request runs under a principal whose `principalLocaleAttribute` value is `hu-HU`
- **THEN** `PrincipalLocaleProvider.getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")`

#### Scenario: No principal bound
- **WHEN** `PrincipalLocaleProvider.getLocale()` is called outside a request scope
- **THEN** it SHALL return `Locale.forLanguageTag(defaultLanguage)` without throwing

#### Scenario: I18nServiceImpl uses the provider
- **WHEN** `PrincipalLocaleProvider` is bound as `LocaleProvider` in Guice or Spring
- **THEN** `I18nServiceImpl` SHALL resolve its optional `@Reference` to this instance and backend messages SHALL be formatted in the principal's resolved locale

### Requirement: `PrincipalLocaleResolver` is a pure helper

The `PrincipalLocaleResolver` class SHALL live in `judo-runtime-core-security` and SHALL have no runtime dependency on the DAO, the `ASM` model, logging side effects that alter behaviour, or the JAX-RS request context. Its public API SHALL be a single method taking the four candidate strings, `supportedLanguages`, `defaultLanguage`, and `browserLanguageCheck`, returning the resolved BCP-47 tag.

#### Scenario: Unit-testable in isolation
- **WHEN** the resolver is invoked with candidate strings only, no DAO or model
- **THEN** it SHALL produce the resolved tag from those inputs alone

#### Scenario: No hidden state
- **WHEN** the resolver is invoked twice with identical inputs
- **THEN** it SHALL produce identical outputs (referential transparency)

### Requirement: Guice and Spring wiring for the four parameters and the LocaleProvider

`judo-runtime-core-guice` and `judo-runtime-core-spring` SHALL each expose bindings for the four parameters and SHALL register `PrincipalLocaleProvider` as the `LocaleProvider` implementation. Spring properties SHALL follow the `judo.platform.*` prefix (e.g. `judo.platform.principalLocaleAttribute`).

#### Scenario: Guice binding
- **WHEN** a Guice-based application binds the four parameters via the JUDO Guice module
- **THEN** `DefaultActorResolver` SHALL receive the parameter values through its builder and `LocaleProvider` SHALL resolve to `PrincipalLocaleProvider`

#### Scenario: Spring property binding
- **WHEN** a Spring-based application sets `judo.platform.principalLocaleAttribute=locale` (and the others)
- **THEN** the autoconfiguration SHALL propagate the values to `DefaultActorResolver` and register `PrincipalLocaleProvider` as a bean of type `LocaleProvider`
