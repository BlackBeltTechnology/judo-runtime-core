## Why

JUDO applications are increasingly multi-lingual, but the runtime has no first-class notion of a
user's effective locale. Each request's language is currently governed by two independent, weakly
coordinated signals:

1. The frontend sets `Accept-Language` from its own state (`l10n-context.tsx`).
2. Backend i18n falls back to `defaultLocale`/JVM because `LocaleProvider` has **no runtime
   implementation** — the optional `@Reference` in `I18nServiceImpl` is unbound in every deployment.

The user's own locale preference (from OIDC `locale` claim, browser hint, or previously stored value)
is never resolved and never propagated to backend messages. There is also no declarative way for a
deployment to say "these are the languages I support; this one is my default."

`JNG-6415` closes that gap **without any model change** (no ecore / ESM / PSM / tatami, no `@Locale`
annotation, no `SET_PRINCIPAL_LOCALE` behaviour, no Keycloak write-back). Crucially, **the backend
never writes the user's stored locale preference** — resolution is purely a read over the request
inputs. Persistence of the user's preferred language is out of scope for this change and is left to
a later capability.

## What Changes

- Introduce four platform system parameters (env vars → OSGi/Spring props), all belonging to the
  `JUDO_PLATFORM_*` family:
  - `JUDO_PLATFORM_PRINCIPAL_LOCALE_ATTRIBUTE` / `principalLocaleAttribute` — actor attribute
    holding the user's BCP-47 locale (may be a mapped attribute the frontend maintains, or an
    OIDC-mapped claim attribute). **Unset ⇒ feature OFF (behaviour identical to today).**
  - `JUDO_PLATFORM_SUPPORTED_LANGUAGES` / `supportedLanguages` — comma-separated list of BCP-47
    tags the deployment offers. Every candidate is filtered against this set (RFC-4647 lookup).
  - `JUDO_PLATFORM_DEFAULT_LANGUAGE` / `defaultLanguage` — terminal fallback and precedence floor.
  - `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK` / `browserLanguageCheck` — boolean; when `true` the
    request `Accept-Language` header becomes the top precedence tier. Defaults to `true`.
- Add a pure, unit-testable helper `PrincipalLocaleResolver` in `judo-runtime-core-security` that
  walks a fixed precedence: **`browser` (if enabled) → `claim` (OIDC `locale`) → `stored` (DB) →
  `default`**. Each tier's candidate is validated against `supportedLanguages` (RFC-4647 lookup —
  `hu` matches `hu-HU`); unsupported tags fall through to the next tier.
- Capture the request `Accept-Language` header in `KeycloakLoginInterceptor` and stash it into the
  principal's `attributes["__acceptLanguage"]` — the only reachable point where the
  `HttpServletRequest` is available before the actor is resolved. Header capture is gated on
  `browserLanguageCheck=true`.
- Provide a runtime-core `PrincipalLocaleProvider implements hu.blackbelt.osgi.i18n.api.LocaleProvider`
  in `judo-runtime-core-dispatcher`. On every `getLocale()` call it reads the three candidate inputs
  from the request-scoped `Context` (browser hint on principal attributes → OIDC claim on principal
  attributes → stored value on the loaded actor payload) and delegates to `PrincipalLocaleResolver`
  to pick one. Reads are O(1) and do not invoke `GET_PRINCIPAL` (avoiding recursion risk during
  error formatting). Binding it to `I18nServiceImpl`'s optional `@Reference` closes the long-standing
  gap where backend messages ignored the user's locale.
- Wire the four parameters and the new `LocaleProvider` in `judo-runtime-core-guice` and
  `judo-runtime-core-spring`.

### Behavioural Constraints

- **Backend is read-only.** No `DAO.update`, no mutation of the actor payload, no Keycloak
  write-back. The stored locale is read; nothing is written. Persisting the user's preferred
  language back to the DB is a follow-up capability.
- **Fixed precedence order.** No `languageOrder` env var in this round (see design D3). Adding it
  later is a self-contained follow-up.
- **Resolution runs per `LocaleProvider.getLocale()` call** — cheap string operations against
  context, no I/O.
- **Browser tier is on by default.** With `browserLanguageCheck=true` (the default) the request
  `Accept-Language` header is captured at the auth interceptor and consulted as the top tier. Set
  `browserLanguageCheck=false` to disable capture and skip the browser tier entirely.
- **Feature is fully off** when `principalLocaleAttribute` is unset — no header capture and the
  `LocaleProvider` returns the configured default (or empty) for every request.

## Capabilities

### New Capabilities
- `principal-locale-resolution`: Read-time resolution of the authenticated user's effective BCP-47
  locale via a configurable precedence (browser → claim → stored → default), filtered against a
  declared supported-language set, exposed to backend i18n through a runtime-bound `LocaleProvider`.
  No persistence.

### Modified Capabilities

## Impact

- **judo-runtime-core-security**: New `PrincipalLocaleResolver` helper (alongside
  `AcceptableClientsParser`) and a `PrincipalLocaleConfig` value object grouping the four settings.
  Pure logic, no dependencies added.
- **judo-runtime-core-security-keycloak-cxf**: `KeycloakLoginInterceptor` captures
  `Accept-Language` into principal attributes when `browserLanguageCheck=true`.
- **judo-runtime-core-dispatcher**: New class `PrincipalLocaleProvider` and a runtime dependency
  on `hu.blackbelt.osgi.i18n:i18n-api`. **`DefaultActorResolver` is unaffected** — no fields, no
  builder params, no logic added.
- **judo-runtime-core-guice**: Bindings for the `PrincipalLocaleConfig` value object and the
  `LocaleProvider` binding.
- **judo-runtime-core-spring**: `@Value` wiring for the four Spring props
  (`judo.platform.principalLocaleAttribute`, `supportedLanguages`, `defaultLanguage`,
  `browserLanguageCheck`) via a `PrincipalLocaleConfig` bean, and the `LocaleProvider` bean.
- **Backward compatible**: `principalLocaleAttribute` unset ⇒ feature entirely off. No interface
  or signature changes. No metamodel changes. No DB writes.
- **Deferred (out of scope for this change)**: persisting the user's preferred language back to
  the DB / OIDC claim (a full "user updates preference" story); judo-platform OSGi wiring
  (`@AttributeDefinition` + PIDS mapping the `JUDO_PLATFORM_*` env vars, plus the
  `PrincipalLocaleProvider` OSGi `@Component` service registration); frontend template changes;
  Keycloak write-back; the `@Locale` annotation and `SET_PRINCIPAL_LOCALE` behaviour.
