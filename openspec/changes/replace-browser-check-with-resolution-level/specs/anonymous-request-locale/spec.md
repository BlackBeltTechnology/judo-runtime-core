## MODIFIED Requirements

### Requirement: `Accept-Language` capture uses a CXF transport interceptor and a thread-local holder

`Accept-Language` capture for anonymous requests SHALL be performed by a CXF
`AbstractPhaseInterceptor` (RECEIVE phase) in `judo-runtime-core-jaxrs-cxf`
(`AcceptLanguageCaptureInterceptor`) that reads the header from
`Message.PROTOCOL_HEADERS` and stores the raw value in a thread-local holder
(`hu.blackbelt.judo.runtime.core.RequestLocaleHolder`, in the core `judo-runtime-core` module).
The holder SHALL be cleared when the header is absent from an incoming request and on fault
handling, so a pooled request thread never carries a stale value into the next request.

`PrincipalLocaleProvider.getLocale()` — in its anonymous branch — SHALL read the raw header from
`RequestLocaleHolder.getAcceptLanguage()` (not from `Context`) and pass it through
`PrincipalLocaleResolver.matchSupportedLanguage(candidate, supportedLanguages)` to produce the
resolved tag.

> Change note: this MODIFIES the parent `add-anonymous-request-locale/design.md` D2 ("Capture
> `Accept-Language` in a JAX-RS `ContainerRequestFilter`") and the associated proposal wording.
> Verified against the implementation: a JAX-RS `ContainerRequestFilter` that stashed the header
> into `Context` would be wiped by `DefaultDispatcher.callOperation` — that method calls
> `context.removeAll()` for exposed (HTTP-entry) operations and repopulates `LOCALE_KEY`/`ACTOR_KEY`
> from the exchange (which the platform / generated REST layer builds, not runtime-core). A
> thread-local set by a transport-phase interceptor survives that reset, which is why the
> implementation shipped the interceptor+holder pattern instead.

#### Scenario: Header captured into the thread-local
- **WHEN** an anonymous request arrives with `Accept-Language: hu-HU,en-US;q=0.7`
- **THEN** `AcceptLanguageCaptureInterceptor.handleMessage` SHALL invoke
  `RequestLocaleHolder.set("hu-HU,en-US;q=0.7")` before any operation dispatch runs

#### Scenario: Holder cleared when header is absent
- **WHEN** an anonymous request arrives with no `Accept-Language` header (and the current thread
  previously served a request that set the holder)
- **THEN** `AcceptLanguageCaptureInterceptor.handleMessage` SHALL invoke
  `RequestLocaleHolder.clear()` so `getAcceptLanguage()` returns `null` inside this request

#### Scenario: Anonymous provider reads the holder
- **WHEN** no principal is bound, `supportedLanguages={en-US, hu-HU}`,
  `localeResolutionLevel=BROWSER`, and `RequestLocaleHolder.getAcceptLanguage()` returns
  `"hu-HU,en-US;q=0.7"`
- **THEN** `PrincipalLocaleProvider.getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")`

### Requirement: App-set `LOCALE_KEY` takes precedence over the captured header

In the anonymous branch of `PrincipalLocaleProvider.getLocale()`, an application-set resolved `Locale` in the request-scoped `Context` under `DefaultDispatcher.LOCALE_KEY` SHALL take precedence over the raw `Accept-Language` value in `RequestLocaleHolder`. The precedence order
for the anonymous branch SHALL be, in strict order:

1. `Context[LOCALE_KEY]` (resolved `Locale`, if present) — respects an explicit application choice.
2. `RequestLocaleHolder.getAcceptLanguage()` filtered through
   `PrincipalLocaleResolver.matchSupportedLanguage(...)` (subject to the `LocaleResolutionLevel`
   ceiling per this change's other requirements).
3. `defaultLanguage` (parsed to `Locale`, or `Optional.empty()` if blank).

> Change note: this MODIFIES the parent `add-anonymous-request-locale/design.md` Open Question
> ("Should the provider also honor an app-set `LOCALE_KEY`... Leaning yes, to be finalized in the
> spec.") to **yes**, matching the implementation (`anonymousApplicationSetLocaleKeyWins` test in
> `PrincipalLocaleProviderTest` already locks the behaviour in). The Open Question is thereby
> closed by this change.

#### Scenario: `LOCALE_KEY` wins over the captured header
- **WHEN** no principal is bound, `Context[LOCALE_KEY]` = `Locale.forLanguageTag("hu-HU")`,
  `RequestLocaleHolder.getAcceptLanguage()` = `"en-US"`, `supportedLanguages={en-US, hu-HU}`
- **THEN** `PrincipalLocaleProvider.getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")`
  (from `LOCALE_KEY`; the captured header SHALL NOT be consulted)

#### Scenario: Header used when `LOCALE_KEY` is absent
- **WHEN** no principal is bound, `Context[LOCALE_KEY]` is absent,
  `RequestLocaleHolder.getAcceptLanguage()` = `"hu-HU"`, `supportedLanguages={en-US, hu-HU}`
- **THEN** `PrincipalLocaleProvider.getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")`

#### Scenario: Default fallback when both are absent
- **WHEN** no principal is bound, `Context[LOCALE_KEY]` is absent,
  `RequestLocaleHolder.getAcceptLanguage()` is `null`, `defaultLanguage=en-US`
- **THEN** `PrincipalLocaleProvider.getLocale()` SHALL return `Locale.forLanguageTag("en-US")`

### Requirement: Spring wiring for anonymous requests provides only the provider

Under **Spring**, `judo-runtime-core-spring` SHALL provide the `LocaleProvider` bean
(`PrincipalLocaleProvider`) that consumes the thread-local holder in its anonymous branch.
Registration of `AcceptLanguageCaptureInterceptor` in a CXF server assembly SHALL be a Spring
Boot starter concern (deferred, out of scope for this change), because
`judo-runtime-core-spring` has no CXF server assembly of its own.

> Change note: this MODIFIES the parent `add-anonymous-request-locale/proposal.md` Impact bullet
> "Wire the filter and the generalized provider in Guice and Spring" — Spring only wires the
> provider in-tree; interceptor registration lives in the Spring Boot starter (or the deployment's
> own CXF configuration).

#### Scenario: Guice wires both the interceptor and the provider
- **WHEN** a Guice-based application assembles the JUDO runtime with a CXF server
- **THEN** `JudoCxfModule` SHALL register `AcceptLanguageCaptureInterceptor` on the CXF endpoint's
  in-interceptor chain
- **AND** `JudoDefaultModule.configureLocaleProvider()` SHALL bind `PrincipalLocaleProvider`

#### Scenario: Spring wires only the provider
- **WHEN** a Spring-based application assembles the JUDO runtime
- **THEN** `JudoDefaultSpringConfiguration.getLocaleProvider(...)` SHALL bind
  `PrincipalLocaleProvider` as a `@Bean`
- **AND** the deployment's Spring Boot starter (or app-level CXF configuration) SHALL be
  responsible for registering `AcceptLanguageCaptureInterceptor`
