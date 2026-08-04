## ADDED Requirements

### Requirement: Anonymous request locale from Accept-Language

For a request with **no bound principal** (anonymous), the request-aware `LocaleProvider` SHALL
resolve the effective locale from the request `Accept-Language` header, filtered against
`supportedLanguages` using RFC-4647 filtering, falling back to `defaultLanguage`. No persistence
SHALL occur.

#### Scenario: Anonymous request with supported Accept-Language
- **WHEN** no principal is bound, `supportedLanguages={en-US, hu-HU}`, and the captured
  `Accept-Language` is `hu-HU,en-US;q=0.7`
- **THEN** `getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")`

#### Scenario: Anonymous request with unsupported Accept-Language
- **WHEN** no principal is bound, `supportedLanguages={en-US}`, `defaultLanguage=en-US`, and the
  captured `Accept-Language` is `de-DE`
- **THEN** `getLocale()` SHALL return `Locale.forLanguageTag("en-US")` (unsupported tag filtered out)

#### Scenario: Anonymous request with no Accept-Language
- **WHEN** no principal is bound and no `Accept-Language` was captured
- **THEN** `getLocale()` SHALL return `Locale.forLanguageTag(defaultLanguage)`

#### Scenario: No persistence for anonymous
- **WHEN** an anonymous request's locale is resolved
- **THEN** no `DAO.update` (or any DB write) SHALL be performed for a locale

### Requirement: Accept-Language capture via a thread-local holder

A transport interceptor SHALL read the request `Accept-Language` header and place it into a
thread-local holder (`RequestLocaleHolder`) that survives the dispatcher's per-operation
`Context` reset, so the request-aware `LocaleProvider` can read it during message formatting. The
interceptor SHALL run for both authenticated and anonymous requests and SHALL set the holder on
every request (clearing it when the header is absent) so a pooled thread never observes a previous
request's value.

> Rationale: `DefaultDispatcher.callOperation` calls `context.removeAll()` for exposed (HTTP-entry)
> operations and repopulates `LOCALE_KEY`/`ACTOR_KEY` from the exchange, so a value stashed into the
> dispatcher `Context` by a JAX-RS filter would be wiped before messages format. A thread-local
> holder set by a CXF transport interceptor (which has HTTP access) survives that reset.

#### Scenario: Header captured into the holder
- **WHEN** a request arrives with `Accept-Language: hu-HU,en-US;q=0.7`
- **THEN** the interceptor SHALL set `RequestLocaleHolder` to the raw header value (case-insensitive
  header lookup)

#### Scenario: Missing header clears the holder
- **WHEN** a request arrives with no `Accept-Language` header on a thread that previously held a value
- **THEN** the interceptor SHALL clear `RequestLocaleHolder`, and downstream resolution SHALL fall
  back to `defaultLanguage`

#### Scenario: Interceptor runs regardless of authentication
- **WHEN** an authenticated request and an anonymous request both carry `Accept-Language`
- **THEN** the interceptor SHALL capture the header in both cases (the authenticated path still
  prefers the principal's locale)

### Requirement: Authenticated behaviour unchanged

When a principal is bound, the request-aware `LocaleProvider` SHALL preserve the exact behaviour of
`add-principal-locale-resolution`: the actor payload (`ACTOR_KEY`) then the principal token
attributes (`PRINCIPAL_KEY`) determine the locale, and the anonymous `Accept-Language` branch SHALL
NOT override it.

#### Scenario: Authenticated principal locale wins over browser
- **WHEN** a principal is bound with a resolved locale `hu-HU` in the actor payload, and the request
  `Accept-Language` is `de-DE`
- **THEN** `getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")` (principal wins; the anonymous
  branch is not consulted)

#### Scenario: Authenticated with no resolvable principal locale
- **WHEN** a principal is bound but neither the actor payload nor the principal attributes contain a
  usable locale
- **THEN** `getLocale()` SHALL fall back to `defaultLanguage` (unchanged from JNG-6415), independent
  of the anonymous branch

### Requirement: Respect an application-set request Locale

The request-aware `LocaleProvider` SHALL, for the anonymous case, prefer an application-set resolved
`Locale` (placed in the request-scoped `Context` under the dispatcher `LOCALE_KEY`) over the raw
captured `Accept-Language`, respecting the explicit application choice; when `LOCALE_KEY` is absent,
it SHALL use the captured `Accept-Language`.

#### Scenario: Application-set locale respected for anonymous
- **WHEN** no principal is bound, the app has set `LOCALE_KEY` to `Locale.forLanguageTag("hu-HU")`,
  and the captured `Accept-Language` is `en-US`
- **THEN** `getLocale()` SHALL return `Locale.forLanguageTag("hu-HU")`

### Requirement: Guice and Spring wiring

`judo-runtime-core-guice` SHALL register the Accept-Language capture interceptor as a CXF
in-interceptor and ensure the request-aware `LocaleProvider` receives `supportedLanguages` and
`defaultLanguage`. `judo-runtime-core-spring` SHALL pass `supportedLanguages` and `defaultLanguage`
to the `LocaleProvider` bean; because runtime-core-spring hosts no CXF server assembly, registering
the capture interceptor for Spring is a Spring Boot starter concern. The `LocaleProvider` binding
introduced by `add-principal-locale-resolution` SHALL continue to resolve to the generalized provider.

#### Scenario: Guice wiring
- **WHEN** a Guice-based application is assembled with the CXF module
- **THEN** the capture interceptor SHALL be registered as a CXF in-interceptor and `LocaleProvider`
  SHALL resolve to the request-aware provider configured with `supportedLanguages` and `defaultLanguage`

#### Scenario: Spring wiring
- **WHEN** a Spring-based application sets `judo.platform.supportedLanguages` and
  `judo.platform.defaultLanguage`
- **THEN** the `LocaleProvider` bean SHALL be the request-aware provider configured with those values
  (the capture interceptor is registered by the Spring Boot starter's CXF assembly)
