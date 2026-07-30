## Why

JUDO applications are increasingly multi-lingual, but the runtime has no first-class notion of a
user's effective locale. Each request's language is currently governed by two independent, weakly
coordinated signals:

1. The frontend sets `Accept-Language` from its own state (`l10n-context.tsx`).
2. Backend i18n falls back to `defaultLocale`/JVM because `LocaleProvider` has **no runtime
   implementation** — the optional `@Reference` in `I18nServiceImpl` is unbound in every deployment.

The user's own locale preference (from OIDC `locale` claim, browser hint, or previously stored value)
is never resolved, never persisted, and never propagated to backend messages. There is also no
declarative way for a deployment to say "these are the languages I support; this one is my default."

`JNG-6415` closes that gap **without any model change** (no ecore / ESM / PSM / tatami, no `@Locale`
annotation, no `SET_PRINCIPAL_LOCALE` behaviour, no Keycloak write-back).

## What Changes

- Introduce four platform system parameters (env vars → OSGi/Spring props), all belonging to the
  `JUDO_PLATFORM_*` family:
  - `JUDO_PLATFORM_PRINCIPAL_LOCALE_ATTRIBUTE` / `principalLocaleAttribute` — mapped actor
    attribute holding the user's BCP-47 locale. **Unset ⇒ feature OFF (behaviour identical to today).**
  - `JUDO_PLATFORM_SUPPORTED_LANGUAGES` / `supportedLanguages` — comma-separated list of BCP-47
    tags the deployment offers. Every candidate is filtered against this set (RFC-4647 lookup).
  - `JUDO_PLATFORM_DEFAULT_LANGUAGE` / `defaultLanguage` — terminal fallback and precedence floor.
  - `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK` / `browserLanguageCheck` — boolean; when `true` the
    request `Accept-Language` header becomes the top precedence tier. Defaults to `true`.
- Add a pure, unit-testable helper `PrincipalLocaleResolver` in `judo-runtime-core-security` that
  walks a fixed precedence: **`browser` (if enabled) → `claim` (OIDC `locale`) → `stored` (DB) →
  `default`**. Each tier's candidate is validated against `supportedLanguages` (RFC-4647 lookup —
  `hu` matches `hu-HU`); unsupported tags fall through to the next tier so an unsupported locale is
  never persisted.
- Capture the request `Accept-Language` header in `KeycloakLoginInterceptor` and stash it into the
  principal's `attributes["__acceptLanguage"]` — the only reachable point where the
  `HttpServletRequest` is available before the resolver runs. Header capture is gated on
  `browserLanguageCheck=true`.
- In `DefaultActorResolver.getActorByClaims`, after the actor entity is loaded, resolve the
  effective locale and — if it differs from the stored value — write it back via the existing
  `DAO.update(actorType, {id, localeAttr}, null)`. Wrapped in `try/catch(WARN)` so a failed refresh
  never breaks authentication. Gated on non-blank `principalLocaleAttribute`.
- Provide a runtime-core `PrincipalLocaleProvider implements hu.blackbelt.osgi.i18n.api.LocaleProvider`
  in `judo-runtime-core-dispatcher` that reads the resolved locale via
  `PrincipalVariableProvider.apply(principalLocaleAttribute)` and binds to `I18nServiceImpl`'s
  optional `@Reference`. This closes the long-standing gap where backend messages ignored the user's
  locale.
- Wire the four parameters and the new `LocaleProvider` in `judo-runtime-core-guice` and
  `judo-runtime-core-spring`.

### Behavioural Constraints

- **`principalLocaleAttribute` must be a mapped (persistent) actor attribute.** If the name does
  not resolve to a mapped attribute, no write happens, the effective locale falls back to
  `defaultLanguage`, a one-time WARN is logged, and authentication is unaffected.
- **Fixed precedence order.** No `languageOrder` env var in this round (see design D3). Adding it
  later is a self-contained follow-up.
- **Refresh on every login** when `resolved != stored`; write is skipped when unchanged.
- **Browser tier is on by default.** With `browserLanguageCheck=true` (the default) the request
  `Accept-Language` header is captured at the auth interceptor and consulted as the top tier. Set
  `browserLanguageCheck=false` to disable capture and skip the browser tier entirely.
- **No Keycloak write-back.** The resolved locale is persisted only in the JUDO RDBMS; the
  Keycloak user profile is not modified.
- **Feature is fully off** when `principalLocaleAttribute` is unset — no header capture, no DB
  refresh, no `LocaleProvider` effect on messages.

## Capabilities

### New Capabilities
- `principal-locale-resolution`: Login-time resolution of the authenticated user's effective
  BCP-47 locale via a configurable precedence (browser → claim → stored → default), filtered
  against a declared supported-language set, persisted via `DAO.update`, and exposed to backend
  i18n through a runtime-bound `LocaleProvider`.

### Modified Capabilities

## Impact

- **judo-runtime-core-security**: New `PrincipalLocaleResolver` helper (alongside
  `AcceptableClientsParser`). Pure logic, no dependencies added.
- **judo-runtime-core-security-keycloak-cxf**: `KeycloakLoginInterceptor` captures
  `Accept-Language` into principal attributes when `browserLanguageCheck=true`.
- **judo-runtime-core-dispatcher**: `DefaultActorResolver` gains four fields
  (`principalLocaleAttribute`, `supportedLanguages`, `defaultLanguage`, `browserLanguageCheck`) and
  a refresh block in `getActorByClaims`. Adds a new class `PrincipalLocaleProvider` and a runtime
  dependency on `hu.blackbelt.osgi.i18n:i18n-api`.
- **judo-runtime-core-guice**: Bindings for the four parameters and the `LocaleProvider` binding.
- **judo-runtime-core-spring**: `@ConfigurationProperties` / `@Value` wiring for the four
  parameters (Spring props `judo.platform.principalLocaleAttribute`, `supportedLanguages`,
  `defaultLanguage`, `browserLanguageCheck`) and the `LocaleProvider` bean.
- **Backward compatible**: `principalLocaleAttribute` unset ⇒ feature entirely off. No interface
  or signature changes. No metamodel changes.
- **Deferred (out of scope for this change)**: judo-platform OSGi wiring
  (`@AttributeDefinition` + PIDS mapping the `JUDO_PLATFORM_*` env vars, plus the
  `PrincipalLocaleProvider` OSGi `@Component` service registration); frontend template changes;
  Keycloak write-back; the `@Locale` annotation and `SET_PRINCIPAL_LOCALE` behaviour.
