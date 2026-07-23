## Context

`add-principal-locale-resolution` (JNG-6415) delivered:
- `PrincipalLocaleResolver` — a pure helper resolving `browser → claim → stored → default` with
  RFC-4647 filtering. Its browser+default tiers work standalone (`claim`/`stored` may be null).
- `PrincipalLocaleProvider implements LocaleProvider` — reads the resolved locale from the
  request-scoped `Context` (`ACTOR_KEY` then `PRINCIPAL_KEY`), falling back to `defaultLanguage`.
- `Accept-Language` capture in `KeycloakLoginInterceptor`, gated on `browserLanguageCheck`, only on
  the authenticated path.

Verified gaps for anonymous requests:
- Anonymous requests carry no bearer token, so `KeycloakLoginInterceptor` never captures their
  `Accept-Language`, and no `JudoPrincipal` is set.
- Nothing in `judo-runtime-core` populates the dispatcher `LOCALE_KEY` (`"__locale"`) in the
  exchange; it is read only by `ExportCall` and otherwise defaults to `defaultLocale`. The
  REST→exchange locale bridge is currently a platform/app concern.
- `judo-runtime-core-jaxrs-cxf-server` already hosts JAX-RS request filters (e.g.
  `SetDefaultContentTypePreMatchContainerRequestFilter`), so a capture filter fits the existing
  pattern in-repo.

## Goals / Non-Goals

**Goals:**
- Localize backend messages for anonymous requests based on `Accept-Language`, filtered to
  `supportedLanguages`, falling back to `defaultLanguage`.
- Reuse the existing pure resolver — no new resolution logic.
- Keep the authenticated behaviour of JNG-6415 byte-for-byte unchanged.
- Keep everything in `judo-runtime-core` (provider + JAX-RS filter + Guice/Spring wiring).

**Non-Goals:**
- Persistence for anonymous users (no DB row, nothing to persist).
- Per-session/cookie memory of an anonymous choice (frontend concern).
- judo-platform OSGi wiring (`@AttributeDefinition`/PIDS, filter service registration) — deferred,
  shared with JNG-6415.
- Any metamodel change.

## Decisions

### D1. Generalize `PrincipalLocaleProvider` (option 1), do not add a second provider

Extend `PrincipalLocaleProvider.getLocale()` with a no-principal branch:
1. actor payload (`ACTOR_KEY`) → principal attributes (`PRINCIPAL_KEY`) — **unchanged** authenticated path.
2. **new:** when neither is present, read the captured request `Accept-Language` from `Context` and
   compute `PrincipalLocaleResolver.resolve(acceptLanguage, null, null, defaultLanguage,
   supportedLanguages, true)`.
3. fall back to `defaultLanguage`, then `Optional.empty()`.

**Rationale:** one request-aware provider is simpler than two and avoids ambiguous double-binding of
`LocaleProvider`. The pure resolver already supports browser-only resolution, so this is a small,
well-tested addition. The provider needs `supportedLanguages` (already parsed for JNG-6415) — it
gains that field via the existing Guice/Spring wiring.

**Alternative considered:** a separate `AnonymousLocaleProvider`. Rejected — `I18nServiceImpl` binds
a single `LocaleProvider`; two providers would need a composite, more machinery for no benefit.

### D2. Capture `Accept-Language` in a JAX-RS `ContainerRequestFilter`

Add `AcceptLanguageCaptureFilter` in `judo-runtime-core-jaxrs-cxf-server`, mirroring
`SetDefaultContentTypePreMatchContainerRequestFilter`. It reads `Accept-Language` (raw header or
`ContainerRequestContext.getAcceptableLanguages()`) and stashes it into the request-scoped `Context`
under a dedicated key the provider reads.

**Rationale:** runs for every request (authenticated + anonymous), needs no token, and lives beside
existing filters in-repo — no judo-platform dependency for the logic. The authenticated path still
prefers the principal's locale (checked first in the provider), so the filter is additive and safe.

**Alternative considered:** populate the dispatcher `LOCALE_KEY` from the header. Rejected as the
primary mechanism because `LOCALE_KEY` carries a resolved `Locale` used by `ExportCall`; overloading
it risks coupling. A dedicated capture key keeps concerns separate (the provider may still honor
`LOCALE_KEY` if already set by the app).

### D3. No persistence, no new parameters

Anonymous resolution reuses `supportedLanguages` and `defaultLanguage` from JNG-6415. There is no
`principalLocaleAttribute` semantics and no `dao.update`. `browserLanguageCheck` continues to gate
the authenticated browser tier; for anonymous, the `Accept-Language` is the only signal and is always
consulted (there is nothing else) — subject to the supported-set filter.

**Rationale:** minimal surface; the anonymous path is purely a read for message i18n.

## Risks / Trade-offs

- **Header spoofing** → An anonymous caller can only select one of the deployment's already-supported
  locales (supported-set filter); no injection surface. Same guarantee as the authenticated browser tier.
- **Double capture (authenticated)** → For authenticated requests both the Keycloak interceptor and
  the JAX-RS filter may capture the header. Harmless: the provider prefers the principal/actor locale,
  and both capture the same value. Documented; no behavioural conflict.
- **Filter ordering** → The capture filter must run early enough that `Context` is available when
  messages are formatted. Mirrors the existing pre-match filter's placement.
- **Non-CXF deployments** → The filter is CXF/JAX-RS specific; other transports would need their own
  capture. Acceptable — CXF is the production REST transport.

## Migration Plan

- **Deploy:** additive. Authenticated behaviour unchanged; anonymous requests begin honoring
  `Accept-Language`. No config change required beyond the JNG-6415 `supportedLanguages`/`defaultLanguage`.
- **Rollback:** unregister the capture filter (or leave `supportedLanguages` empty) → anonymous
  reverts to default/JVM locale.

## Open Questions

- Should the provider also honor an app-set `LOCALE_KEY` (resolved `Locale`) if present, as a higher
  precedence than the raw captured header? Leaning yes (respect explicit app choice), to be finalized
  in the spec.
