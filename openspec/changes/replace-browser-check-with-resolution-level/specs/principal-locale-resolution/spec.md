## MODIFIED Requirements

### Requirement: Platform parameters for principal locale resolution

The system SHALL support four platform parameters that configure principal locale resolution.
Each parameter SHALL be settable via a `JUDO_PLATFORM_*` environment variable (mapped by the
follow-up platform change) or via the corresponding Guice / Spring configuration property in this
change.

| Env var | Property | Type | Default |
|---|---|---|---|
| `JUDO_PLATFORM_PRINCIPAL_LOCALE_ATTRIBUTE` | `principalLocaleAttribute` | String | unset ⇒ feature OFF |
| `JUDO_PLATFORM_SUPPORTED_LANGUAGES` | `supportedLanguages` | comma-separated BCP-47 list | `{defaultLanguage}` |
| `JUDO_PLATFORM_DEFAULT_LANGUAGE` | `defaultLanguage` | BCP-47 tag | JVM default / `en-US` |
| `JUDO_PLATFORM_LOCALE_RESOLUTION_LEVEL` | `localeResolutionLevel` | `LocaleResolutionLevel` enum | `BROWSER` |

> Change note: the previous boolean `browserLanguageCheck` / `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK`
> is **removed outright** (no back-compat alias — JNG-6415 is unreleased). Old-value mapping for
> operators: `true → BROWSER`, `false → IDENTITY_PROVIDER`.

#### Scenario: All parameters unset
- **WHEN** none of the four parameters are configured
- **THEN** the runtime SHALL behave identically to the pre-change baseline: no `Accept-Language`
  capture and the `LocaleProvider` reference stays unbound

#### Scenario: Only `principalLocaleAttribute` set
- **WHEN** `principalLocaleAttribute=locale` and the other three parameters are unset
- **THEN** `supportedLanguages` SHALL default to `{defaultLanguage}`, `defaultLanguage` SHALL
  default to the JVM default (or `en-US` if the JVM default is not a valid BCP-47 tag), and
  `localeResolutionLevel` SHALL default to `BROWSER`

### Requirement: Fixed precedence order

The `PrincipalLocaleResolver` helper SHALL walk candidate locales in a fixed order: `browser` →
`claim` → `stored` → `default`. Each tier's candidate SHALL be validated against
`supportedLanguages` via RFC-4647 lookup; a candidate that does not match SHALL be skipped so the
next tier is consulted. The `default` tier SHALL always resolve. The `LocaleResolutionLevel`
ceiling gates which tiers are consulted at all (see the requirement below).

#### Scenario: Browser tier wins when the ceiling allows it and it is supported
- **WHEN** `localeResolutionLevel=BROWSER`, `supportedLanguages={en-US, hu-HU}`,
  `Accept-Language: hu`, claim locale `en-US`, stored locale `en-US`
- **THEN** the resolver SHALL return `hu-HU` (RFC-4647 lookup: `hu` → `hu-HU`)

#### Scenario: Claim tier wins when ceiling is IDENTITY_PROVIDER
- **WHEN** `localeResolutionLevel=IDENTITY_PROVIDER`, `supportedLanguages={en-US, hu-HU}`,
  `Accept-Language: hu` (ignored — above ceiling), claim locale `hu-HU`, stored locale `en-US`
- **THEN** the resolver SHALL return `hu-HU` from the claim; the `Accept-Language` header SHALL
  NOT be consulted

#### Scenario: Unsupported browser tag falls through
- **WHEN** `localeResolutionLevel=BROWSER`, `supportedLanguages={en-US}`, `Accept-Language: hu-HU`,
  claim locale `en-US`, stored locale null
- **THEN** the resolver SHALL skip the browser tier (candidate unsupported) and return `en-US`
  from the claim tier

#### Scenario: Stored tier wins when higher tiers are above the ceiling
- **WHEN** `localeResolutionLevel=PRINCIPAL`, `supportedLanguages={en-US, hu-HU}`,
  `Accept-Language: hu-HU` (ignored — above ceiling), claim locale `en-US` (ignored — above
  ceiling), stored locale `hu-HU`
- **THEN** the resolver SHALL return `hu-HU` from the stored tier; neither the header nor the
  claim SHALL be consulted

#### Scenario: Default terminal fallback
- **WHEN** `supportedLanguages={en-US, hu-HU}`, `defaultLanguage=en-US`, all higher tiers empty
  or above the ceiling
- **THEN** the resolver SHALL return `en-US`

### Requirement: Browser tier gated by the resolution level

The `Accept-Language` header SHALL be consulted only when the configured `localeResolutionLevel`
satisfies `level.includes(BROWSER)`. When the level is `IDENTITY_PROVIDER` or `PRINCIPAL`, the
`KeycloakLoginInterceptor` SHALL NOT read the header, the authenticated tier walk SHALL omit the
browser tier, **and** the anonymous request branch of `PrincipalLocaleProvider.getLocale()` SHALL
NOT consult `RequestLocaleHolder` (the last part corrects a pre-change inconsistency where the
anonymous branch ignored the toggle).

#### Scenario: Level PRINCIPAL — header not captured
- **WHEN** `localeResolutionLevel=PRINCIPAL` and a request arrives with `Accept-Language: hu-HU`
- **THEN** `JudoPrincipal.attributes` SHALL NOT contain the key `__acceptLanguage`

#### Scenario: Level IDENTITY_PROVIDER — header not captured
- **WHEN** `localeResolutionLevel=IDENTITY_PROVIDER` and a request arrives with
  `Accept-Language: hu-HU`
- **THEN** `JudoPrincipal.attributes` SHALL NOT contain the key `__acceptLanguage`

#### Scenario: Level BROWSER — header captured
- **WHEN** `localeResolutionLevel=BROWSER` and a request arrives with
  `Accept-Language: hu-HU,en-US;q=0.7`
- **THEN** `JudoPrincipal.attributes["__acceptLanguage"]` SHALL be set to the raw header value
- **AND** the resolver SHALL parse quality-ordered ranges and consult them against
  `supportedLanguages` in order

#### Scenario: Level BROWSER but header absent
- **WHEN** `localeResolutionLevel=BROWSER` and the request has no `Accept-Language` header
- **THEN** the browser tier SHALL be empty and the resolver SHALL fall through to the claim tier

#### Scenario: Anonymous request honours the ceiling
- **WHEN** no principal is bound, `localeResolutionLevel=IDENTITY_PROVIDER`,
  `supportedLanguages={en-US, hu-HU}`, `defaultLanguage=en-US`, and `RequestLocaleHolder` contains
  `hu-HU`
- **THEN** `PrincipalLocaleProvider.getLocale()` SHALL return `Locale.forLanguageTag("en-US")` —
  the anonymous browser hint SHALL NOT be consulted because the ceiling excludes it

#### Scenario: Anonymous request with BROWSER ceiling reads the holder
- **WHEN** no principal is bound, `localeResolutionLevel=BROWSER`,
  `supportedLanguages={en-US, hu-HU}`, and `RequestLocaleHolder` contains `hu-HU`
- **THEN** `PrincipalLocaleProvider.getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")`

### Requirement: `PrincipalLocaleResolver` is a pure helper

The `PrincipalLocaleResolver` class SHALL live in `judo-runtime-core-security` and SHALL have no
runtime dependency on the DAO, the `ASM` model, logging side effects that alter behaviour, or the
JAX-RS request context. Its public API SHALL consist of three static methods and one constant:

- `resolve(acceptLanguageHeader, claimLocale, storedLocale, defaultLanguage, supportedLanguages,
  LocaleResolutionLevel level)` — the full tier walker used by the authenticated login refresh.
  `null` level SHALL be treated as `LocaleResolutionLevel.DEFAULT` (`BROWSER`).
- `matchSupportedLanguage(candidate, supportedLanguages)` — a single-tier RFC-4647 filter used
  by the anonymous branch of `PrincipalLocaleProvider`.
- `parseSupportedLanguages(csv)` — CSV-to-`Set<String>` helper (memoised once per
  `PrincipalLocaleConfig` instance).
- `ACCEPT_LANGUAGE_ATTRIBUTE = "__acceptLanguage"` — the runtime-internal principal-attribute key
  under which the raw `Accept-Language` header is stashed by `KeycloakLoginInterceptor`.

> Change note: this MODIFIES the parent spec's "single method" wording, which undercounted the
> resolver's public surface. The `matchSupportedLanguage` and `parseSupportedLanguages` helpers
> were required by `add-anonymous-request-locale` / `consolidate-locale-config-object`; the
> parent spec was never updated to reflect them.

#### Scenario: Unit-testable in isolation
- **WHEN** the resolver is invoked with candidate strings only, no DAO or model
- **THEN** it SHALL produce the resolved tag from those inputs alone

#### Scenario: No hidden state
- **WHEN** the resolver is invoked twice with identical inputs
- **THEN** it SHALL produce identical outputs (referential transparency)

#### Scenario: Null level treated as default
- **WHEN** the resolver is invoked with `level=null`
- **THEN** it SHALL behave as if `LocaleResolutionLevel.BROWSER` were passed

### Requirement: Feature gate on `principalLocaleAttribute` scopes to authenticated paths only

Setting `principalLocaleAttribute` to null, empty, or whitespace-only SHALL disable the
**authenticated** paths only:

- `KeycloakLoginInterceptor` SHALL NOT read the `Accept-Language` header from the
  `HttpServletRequest` (subject also to the `LocaleResolutionLevel` ceiling per the
  "Browser tier gated by the resolution level" requirement above).
- `DefaultActorResolver.getActorByClaims` SHALL NOT call `PrincipalLocaleResolver.resolve(...)`
  and SHALL NOT mutate the in-request actor payload's locale attribute.
- The `resolvePrincipalLocaleTag()` helper inside `PrincipalLocaleProvider.getLocale()` SHALL
  short-circuit and return no candidate.

The **anonymous** path (`PrincipalLocaleProvider.getLocale()` when no principal is bound) SHALL
NOT be affected by the `principalLocaleAttribute` gate. It has its own independent enabling
condition (a non-empty `supportedLanguages` set combined with a captured `Accept-Language`
header, subject to the `LocaleResolutionLevel` ceiling).

> Change note: this MODIFIES the parent spec's `Feature gate on principalLocaleAttribute`
> requirement, which asserted `PrincipalLocaleProvider.getLocale()` "SHALL return the default
> (identical to unbound LocaleProvider)" whenever the attribute is unset. Verified against the
> implementation: the gate is scoped to the authenticated tier walk, and the anonymous path
> (introduced by `add-anonymous-request-locale`) resolves independently.

#### Scenario: Feature disabled, authenticated request
- **WHEN** `principalLocaleAttribute` is unset
- **AND** an authenticated request arrives with an OIDC `locale` claim `hu-HU` and
  `Accept-Language: hu-HU`
- **THEN** `KeycloakLoginInterceptor` SHALL NOT read the header (given ceiling `BROWSER`, the
  gate that disables capture here is the unset `principalLocaleAttribute`, not the ceiling)
- **AND** `DefaultActorResolver.getActorByClaims` SHALL NOT invoke
  `PrincipalLocaleResolver.resolve(...)`
- **AND** `PrincipalLocaleProvider.getLocale()` SHALL return
  `Locale.forLanguageTag(defaultLanguage)` (or `Optional.empty()` if default is blank)

#### Scenario: Feature disabled, anonymous request
- **WHEN** `principalLocaleAttribute` is unset, `supportedLanguages={en-US, hu-HU}`,
  `localeResolutionLevel=BROWSER`
- **AND** an anonymous request arrives with `Accept-Language: hu-HU`
- **THEN** `PrincipalLocaleProvider.getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")` —
  the anonymous path is unaffected by the `principalLocaleAttribute` gate

### Requirement: Non-mapped or transient `principalLocaleAttribute` skips the write without forcing a default

`DefaultActorResolver.refreshActorLocale` SHALL treat a non-mapped or `transient`
`principalLocaleAttribute` as a write-suppression signal, not as a trigger to force a default
locale. Specifically, when `principalLocaleAttribute` names an attribute that does not resolve to
a mapped, non-`transient` attribute on the actor `EClass`, `refreshActorLocale` SHALL:

- skip the `DAO.update` write (via `computeLocaleRefresh` returning `null` on `!persistable`);
- **not** mutate the in-request actor payload's locale attribute — whatever value the DAO returned
  (including `null`) SHALL remain as-is;
- log a single WARN naming the offending attribute and actor type (deduplicated one per JVM
  lifetime, per actor type, via `localeRefreshWarned` `Set`);
- allow authentication to proceed unaffected.

`PrincipalLocaleProvider.getLocale()` SHALL subsequently compute the effective locale from the
untouched actor payload via the usual tier walk (browser → claim → stored → default). The
runtime SHALL NOT force the actor payload's locale attribute to `defaultLanguage`.

> Change note: this MODIFIES the parent spec's `principalLocaleAttribute must be a mapped
> attribute` requirement, which said the request "SHALL proceed with the resolved locale
> determined via the fallback chain" — vague and interpreted by the living reference doc as "the
> effective locale falls back to defaultLanguage", which is not what the code does. This tightens
> the contract to the actual behaviour.

#### Scenario: Attribute not present on actor EClass
- **WHEN** `principalLocaleAttribute=locale` but the actor `EClass` has no attribute named
  `locale`, and the DAO-loaded actor payload has no `locale` key
- **THEN** no `DAO.update` call SHALL be made
- **AND** the actor payload SHALL still have no `locale` key after `refreshActorLocale` returns
- **AND** a WARN SHALL be logged once per JVM lifetime, per actor type
- **AND** `PrincipalLocaleProvider.getLocale()` SHALL return
  `Locale.forLanguageTag(defaultLanguage)` (there is no stored value to consult, and
  claim/browser tiers are computed independently)

#### Scenario: Attribute exists on EClass but is transient
- **WHEN** `principalLocaleAttribute=locale` and the actor has a transient `locale` attribute,
  and the DAO-loaded actor payload happens to have `locale="en-US"` (a stale value from a
  previous session's claim copy)
- **THEN** no `DAO.update` call SHALL be made
- **AND** the actor payload SHALL still have `locale="en-US"` after `refreshActorLocale` returns
  (the runtime SHALL NOT overwrite it with `defaultLanguage`)
- **AND** a WARN SHALL be logged once

### Requirement: `PrincipalLocaleProvider` reads directly from Context (no dispatch)

`PrincipalLocaleProvider.getLocale()` SHALL read the resolved locale from the request-scoped
`Context` — first from the loaded actor payload under `Dispatcher.ACTOR_KEY`, then from the
principal's token attributes under `Dispatcher.PRINCIPAL_KEY` — and SHALL NOT invoke
`PrincipalVariableProvider.apply(...)` or trigger a `GET_PRINCIPAL` operation dispatch. When no
candidate resolves through the tier walk (authenticated or anonymous), it SHALL return
`Locale.forLanguageTag(defaultLanguage)` (or `Optional.empty()` when `defaultLanguage` is blank).

> Change note: this MODIFIES the parent spec's `Backend messages follow the resolved locale via
> PrincipalLocaleProvider` requirement, which said the provider reads via
> `PrincipalVariableProvider.apply(principalLocaleAttribute)`. Verified against the
> implementation and already noted as an intentional deviation in
> `add-principal-locale-resolution/tasks.md` §6.3: reading `Context` directly is O(1) and
> side-effect-free, whereas `apply(...)` would trigger a full `GET_PRINCIPAL` dispatch on every
> message-key lookup and risk recursion during error formatting.

#### Scenario: Actor payload is preferred over principal attributes
- **WHEN** the request runs under a principal whose token attributes contain `locale="en-US"`
  and whose loaded actor payload contains `locale="hu-HU"`
- **THEN** `PrincipalLocaleProvider.getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")`
  from the actor payload

#### Scenario: No dispatch during message-key lookup
- **WHEN** `PrincipalLocaleProvider.getLocale()` is invoked (as `I18nServiceImpl` does per
  message-key lookup)
- **THEN** it SHALL NOT invoke `GET_PRINCIPAL`, SHALL NOT dispatch any operation, and SHALL NOT
  read from the DAO

### Requirement: Guice and Spring wiring for the four parameters and the LocaleProvider

`judo-runtime-core-guice` and `judo-runtime-core-spring` SHALL each expose bindings for the four
parameters (grouped into `PrincipalLocaleConfig`) and SHALL register `PrincipalLocaleProvider`
as the `LocaleProvider` implementation. The `localeResolutionLevel` field on
`PrincipalLocaleConfig` SHALL default to `LocaleResolutionLevel.BROWSER`.

Under **Guice**, `KeycloakLoginInterceptorProvider`
(`judo-runtime-core-guice-keycloak`) SHALL inject `PrincipalLocaleConfig` as
`@Inject(optional=true)` and thread `localeConfig.getLocaleResolutionLevel()` into the
`KeycloakLoginInterceptor` builder. Under **Spring**, `getPrincipalLocaleConfig(...)` SHALL read
`judo.platform.localeResolutionLevel` as a `String` (default `BROWSER`) and pass it through
`LocaleResolutionLevel.parse(...)`.

> Change note: this closes a pre-change bug where the Guice–Keycloak provider ignored the toggle
> entirely (no field, silently defaulted to browser-on). It is not merely a spec rewording.

#### Scenario: Guice binding of the enum ceiling
- **WHEN** a Guice-based application configures
  `JudoDefaultModuleConfiguration.builder().actorResolverLocaleResolutionLevel(PRINCIPAL).build()`
- **THEN** the `PrincipalLocaleConfig` binding SHALL expose `PRINCIPAL` as the level
- **AND** the constructed `KeycloakLoginInterceptor` SHALL receive `PRINCIPAL` (i.e., its
  `captureBrowserLanguage` shall be `false`), so no `Accept-Language` header is captured

#### Scenario: Spring binding of the enum ceiling
- **WHEN** a Spring-based application sets `judo.platform.localeResolutionLevel=identity_provider`
  (any case, any surrounding whitespace)
- **THEN** the `PrincipalLocaleConfig` bean SHALL expose `IDENTITY_PROVIDER` as the level

#### Scenario: Guice-Keycloak provider without the config binding
- **WHEN** `PrincipalLocaleConfig` is not bound in the injector
- **THEN** `KeycloakLoginInterceptorProvider` SHALL construct the interceptor with `null` level,
  which the interceptor SHALL interpret as the `BROWSER` default (capture enabled)

## ADDED Requirements

### Requirement: `LocaleResolutionLevel` enum shape

`judo-runtime-core-security` SHALL define a public enum
`hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel` with **exactly three** constants
declared in **ascending precedence order**: `PRINCIPAL`, `IDENTITY_PROVIDER`, `BROWSER`. The enum
SHALL expose:

- `public static final LocaleResolutionLevel DEFAULT = BROWSER;`
- `public boolean includes(LocaleResolutionLevel tier)` returning
  `this.ordinal() >= tier.ordinal()`.
- `public static LocaleResolutionLevel parse(String value)` — lenient, case-insensitive
  (`trim().toUpperCase(Locale.ROOT)`); `null` / blank / unrecognised input SHALL return `DEFAULT`
  and SHALL NOT throw.

#### Scenario: Ceiling semantics — BROWSER includes all
- **WHEN** the level is `BROWSER`
- **THEN** `includes(PRINCIPAL)`, `includes(IDENTITY_PROVIDER)`, and `includes(BROWSER)` SHALL all
  return `true`

#### Scenario: Ceiling semantics — IDENTITY_PROVIDER excludes BROWSER
- **WHEN** the level is `IDENTITY_PROVIDER`
- **THEN** `includes(PRINCIPAL)` and `includes(IDENTITY_PROVIDER)` SHALL return `true`; and
  `includes(BROWSER)` SHALL return `false`

#### Scenario: Ceiling semantics — PRINCIPAL excludes all higher tiers
- **WHEN** the level is `PRINCIPAL`
- **THEN** `includes(PRINCIPAL)` SHALL return `true`; `includes(IDENTITY_PROVIDER)` and
  `includes(BROWSER)` SHALL return `false`

#### Scenario: parse tolerates case and whitespace
- **WHEN** `parse("  browser  ")`, `parse("BROWSER")`, `parse("Browser")` are called
- **THEN** each SHALL return `LocaleResolutionLevel.BROWSER`

#### Scenario: parse falls back to DEFAULT on unknown / blank / null
- **WHEN** `parse(null)`, `parse("")`, `parse("   ")`, or `parse("foobar")` is called
- **THEN** each SHALL return `LocaleResolutionLevel.DEFAULT` (`BROWSER`) and SHALL NOT throw

### Requirement: `KeycloakLoginInterceptor` takes the enum

`KeycloakLoginInterceptor`'s `@Builder` SHALL accept a `LocaleResolutionLevel localeResolutionLevel`
parameter (replacing the previous `Boolean browserLanguageCheck`). Internally the interceptor
SHALL derive a boolean `captureBrowserLanguage = level == null || level.includes(BROWSER)` and
pass that boolean to the package-static helper `captureAcceptLanguage(attributes, request,
captureBrowserLanguage)`. The static helper's signature (`Map`, `HttpServletRequest`, `boolean`)
SHALL be preserved so existing tests calling it directly continue to compile.

#### Scenario: Enum ceiling propagates to the helper
- **WHEN** the interceptor is built with `.localeResolutionLevel(LocaleResolutionLevel.PRINCIPAL)`
- **THEN** the helper SHALL be invoked with `captureBrowserLanguage=false`
- **AND** `attributes` SHALL NOT contain the key `__acceptLanguage`

#### Scenario: Null level defaults to capture-on
- **WHEN** the interceptor is built with `.localeResolutionLevel(null)`
- **THEN** the helper SHALL be invoked with `captureBrowserLanguage=true`

### Requirement: Backend operations may inject `LocaleProvider`

The `LocaleProvider` binding SHALL be a plain injectable singleton reachable from custom backend operations supplied as `Function<Payload,Payload>` via `DispatcherFunctionProvider.getSdkFunctions()` in both `judo-runtime-core-guice` and `judo-runtime-core-spring`. An operation
implementation SHALL be able to `@Inject LocaleProvider` (or `@Inject I18nService`) at
construction time and call `getLocale()` inside its `apply(Payload)` implementation to obtain the
principal's resolved locale at operation-call time.

#### Scenario: Injected provider reflects the resolution level
- **WHEN** the runtime is assembled with `localeResolutionLevel=BROWSER`, an authenticated
  request arrives with a supported `Accept-Language` header, and a backend operation calls
  `localeProvider.getLocale()`
- **THEN** the returned `Optional<Locale>` SHALL match the header (RFC-4647 filtered against
  `supportedLanguages`)

#### Scenario: Ceiling change flips the return
- **WHEN** the same request is served by an injector assembled with
  `localeResolutionLevel=IDENTITY_PROVIDER`, and the OIDC `locale` claim differs from the header
- **THEN** `getLocale()` SHALL return the claim locale, not the header locale
