## Why

`add-principal-locale-resolution` (JNG-6415) resolves and persists an **authenticated** user's
locale and surfaces it to backend message i18n. It deliberately does nothing for **anonymous**
requests, because an anonymous actor has no principal, no token claim, and no DB user row — so the
`claim`, `stored`, and persistence tiers are all undefined.

But anonymous users still deserve localized backend messages (validation errors, exceptions,
templates). The only meaningful locale signal for them is the request's `Accept-Language` header —
which the frontend already sets from its active locale. Today nothing in `judo-runtime-core`
captures `Accept-Language` for non-authenticated requests, and `PrincipalLocaleProvider` returns the
default when no principal is bound. As a result, anonymous backend messages always fall back to the
configured / JVM locale regardless of the browser or the frontend's chosen language.

This change makes the backend honor the request locale for anonymous requests — a **request-scoped**
locale with **no persistence** — closing the gap so a frontend language switch drives anonymous
backend messages for free.

## What Changes

- **Generalize `PrincipalLocaleProvider` into a request-aware `LocaleProvider`** (option 1): when a
  principal is bound it behaves exactly as today (principal's resolved locale); when **no principal**
  is bound it resolves the request `Accept-Language` against `supportedLanguages` (RFC-4647), falling
  back to `defaultLanguage`. Reuses the existing pure `PrincipalLocaleResolver` browser+default tiers
  (`resolve(acceptLanguage, null, null, defaultLanguage, supportedLanguages, true)`) — no new
  resolution logic.
- **Add an `Accept-Language` capture filter for all requests** — a JAX-RS
  `ContainerRequestFilter` in `judo-runtime-core-jaxrs-cxf-server` (mirroring the existing
  `SetDefaultContentTypePreMatchContainerRequestFilter`) that reads the header and stashes it into
  the request-scoped `Context` so the provider can read it O(1). This runs for authenticated and
  anonymous requests alike; the authenticated path continues to prefer the principal's locale.
- **No persistence for anonymous.** There is no DB row and no `principalLocaleAttribute` semantics
  for anonymous requests; the resolved locale affects message i18n only.
- Wire the filter and the generalized provider in Guice and Spring.

### Constraints

- **No change to the authenticated behaviour** of `add-principal-locale-resolution`: when a
  principal is present, the actor/principal locale still wins over the browser tier's own default;
  the browser hint captured at the Keycloak interceptor remains the top tier for authenticated users
  per JNG-6415.
- **Supported-set filtering still applies:** an anonymous `Accept-Language` that matches no supported
  tag falls back to `defaultLanguage`, so an unsupported locale is never used.
- **Feature-off parity:** with no `supportedLanguages`/`defaultLanguage` configured, behaviour is
  identical to today (default / JVM locale).

## Capabilities

### New Capabilities
- `anonymous-request-locale`: Request-scoped resolution of the effective locale for anonymous
  (unauthenticated) requests from the `Accept-Language` header, filtered against the supported
  languages, exposed to backend message i18n via the request-aware `LocaleProvider`, without
  persistence.

### Modified Capabilities
- `principal-locale-resolution`: `PrincipalLocaleProvider` is generalized to also serve the
  no-principal (anonymous) case; the authenticated behaviour is unchanged.

## Impact

- **judo-runtime-core-dispatcher**: `PrincipalLocaleProvider` gains a no-principal branch that reads
  the request `Accept-Language` from `Context` and resolves via `PrincipalLocaleResolver`.
- **judo-runtime-core-jaxrs-cxf-server** (and possibly `judo-runtime-core-jaxrs`): new
  `AcceptLanguageCaptureFilter` (`ContainerRequestFilter`) placing the header into the exchange /
  `Context`.
- **judo-runtime-core-guice** + **judo-runtime-core-spring**: register the capture filter and the
  generalized provider (the provider binding already exists from JNG-6415).
- **Backward compatible**: authenticated behaviour unchanged; anonymous behaviour only improves
  (honors `Accept-Language` instead of always defaulting). No metamodel changes, no persistence.
- **Deferred**: judo-platform OSGi wiring for the filter registration and the shared
  `JUDO_PLATFORM_*` env-var mapping (same follow-up as JNG-6415).
