## Context

Two independent facts, verified against the live tree, motivate the design:

1. **The login choke-point** `DefaultActorResolver.authenticateActor(exchange)` →
   `authenticateByPrincipal` → `getActorByClaims` is the single point where the actor entity is
   loaded from the JUDO DB (`dao.search(actorType, …)`). This is the *only* place that runs both
   "authenticated" AND "user exists in DB" — the natural gate for a persistent locale refresh.
2. **`Accept-Language` is only reachable at the auth interceptor.** `KeycloakLoginInterceptor`
   holds the `HttpServletRequest` (`message.get("HTTP.REQUEST")`). Downstream, `Context` is a bare
   key/value store — no request access. Any header signal must be captured at the interceptor and
   ridden into `JudoPrincipal.attributes` (String-valued, to survive the String-only claims filter
   at `DefaultActorResolver.java:110-113`).

Additional context:

- **Principal attributes are split.** Mapped attributes live in the JUDO RDBMS (readable via
  `dao.search`/`getByIdentifier`, writable via `dao.update`). `transient`-annotated attributes are
  copied from Keycloak claims on every request and are not persisted by JUDO. `getPrincipal()`
  returns the merge.
- **`LocaleProvider` has no runtime implementation today.** The interface exists
  (`hu.blackbelt.osgi.i18n.api.LocaleProvider`) and is consumed as an optional `@Reference` by
  `I18nServiceImpl`, `EnumI18nServiceImpl`, and the Hibernate `MessageInterpolator`, but no
  runtime-core class implements it. Backend i18n therefore falls back to `defaultLocale` / JVM in
  every deployment.
- **`DAO.update(EClass, Payload, QueryCustomizer)`** is already implemented in
  `AbstractRdbmsDAO.update` (`:228`). No new DAO surface is required.
- **Wiring pattern is established.** The proven 5-layer flow used for `checkMappedActors` and
  `acceptableClients` (env var → OSGi `@AttributeDefinition` + PIDS → activator config → Guice/Spring
  binding → `DefaultActorResolver` field) is copied verbatim for the four new parameters. Only the
  runtime-core layers (Guice + Spring binding + resolver field) are in scope for this change; the
  judo-platform OSGi wiring is a follow-up.

## Goals / Non-Goals

**Goals:**
- Resolve the authenticated user's effective BCP-47 locale via a fixed, documented precedence
  (browser → claim → stored → default) and persist it in the JUDO DB.
- Filter every candidate against a deployment-declared `supportedLanguages` set using RFC-4647
  lookup, so an unsupported locale is never persisted.
- Close the `LocaleProvider`-unbound gap so backend i18n messages follow the resolved locale.
- Keep the precedence logic pure and unit-testable, isolated from I/O and DAO calls.
- Zero behavioural change when `principalLocaleAttribute` is unset (feature-off default).
- No metamodel changes; no `judo-dispatcher-api` changes; no new DAO surface.
- Never break authentication if the locale refresh fails — WARN and continue.

**Non-Goals:**
- Configurable precedence order (`languageOrder` env var). Fixed order this round; adding it later
  is a self-contained follow-up.
- Keycloak write-back (updating the user's Keycloak profile locale).
- judo-platform OSGi wiring (`@AttributeDefinition` + PIDS, `LocaleProvider` OSGi service
  registration). Deferred to a follow-up change.
- Metamodel-level `@Locale` annotation, `SET_PRINCIPAL_LOCALE` behaviour, PSM/ESM/ecore edits,
  tatami transforms.
- Frontend template changes; anything under `judo-ui-*`.
- Migration/backfill of existing DB rows (feature is opt-in; existing rows without a stored locale
  simply follow the fallback chain at first login).

## Decisions

### D1. Refresh inside `getActorByClaims`, not a new `AuthenticationInterceptor`

Add the locale-refresh block inside `DefaultActorResolver.getActorByClaims`, immediately after the
`dao.search(...)` result is loaded and before it is returned. Gated on non-blank
`principalLocaleAttribute`. Wrapped in `try/catch(WARN)` so a failed write cannot break auth.

**Rationale:** the actor entity (identifier + stored locale) is already in hand at this point.
Reusing it means no second DAO round-trip and no duplication of the USERNAME/EMAIL/identifier
lookup logic. This is also the exact place where the "authenticated AND exists in DB" invariant
holds — earlier interceptor hooks run before the DB lookup.

**Alternative considered:** a framework-provided `AuthenticationInterceptor` (matching the
per-app `authentication-event-interceptor` blueprint some deployments already use for JIT
provisioning / login tracking). Rejected here because it would either duplicate the actor lookup
or race with it, and the concern is not app-specific — it belongs in the framework's core login
path.

### D2. Refresh on every login when `resolved != stored`

Write only if the newly resolved value differs from the stored value; skip otherwise. This
supersedes an earlier "seed-once, never overwrite" draft.

**Rationale:** with the precedence in place, `stored` is just one tier among four. Locking it in
on first login would make the browser and claim tiers effectively dead code after the first
authentication. Refresh-on-change gives the tiers meaning while keeping writes minimal.

### D3. Pure helper with fixed order + `browserLanguageCheck` boolean

`PrincipalLocaleResolver` lives in `judo-runtime-core-security` (next to `AcceptableClientsParser`)
as a pure class — no DAO, no logger side-effects, no request access. It exposes a single method
taking the four candidate strings, the supported set, the default, and the browser gate. The tier
order is hard-coded: `browser` (only if gate true) → `claim` → `stored` → `default`. RFC-4647
lookup is done per tier; unsupported tags fall through.

**Rationale:** the helper is trivially unit-testable, has no framework dependency, and is small
enough (~one file) that swapping in a configurable order later is a bounded change (see D3-alt).
Fixed order also avoids a whole class of misconfiguration bugs.

**Alternative considered (D3-alt):** a `languageOrder` env var. Rejected for this round to keep
the surface minimal; if a real customer need appears, the retrofit adds one field and a parser
without disturbing the tier implementations.

### D3a. `principalLocaleAttribute` must be a mapped attribute

At refresh time, if `principalLocaleAttribute` does not resolve to a mapped attribute on the
actor's `EClass`, the refresh is skipped (no `DAO.update` call), the effective locale falls back
to `defaultLanguage`, and a one-time WARN is logged. Authentication is unaffected.

**Rationale:** `transient` attributes have no JUDO column to write; a `DAO.update` on a transient
attribute is a no-op / failure. Detecting this at the point of use — rather than at construction
— keeps the resolver decoupled from the ASM model wiring.

### D5. `Accept-Language` capture in `KeycloakLoginInterceptor`

When `browserLanguageCheck=true` (the default), `KeycloakLoginInterceptor.handleMessage` reads
the request's `Accept-Language` header and puts it into `attributes["__acceptLanguage"]` before
building the `JudoPrincipal`. The double-underscore prefix marks it as a runtime-internal key
that will never collide with a Keycloak claim attribute (Keycloak claim names never begin with
`__`). Setting the gate to `false` disables the read entirely.

**Rationale:** this is the only reachable point that holds the `HttpServletRequest`, and stashing
into `attributes` is the only route that survives the String-only claims filter in
`DefaultActorResolver` (L110-113). Default-on matches the pragmatic expectation that a user's
browser preference should influence locale unless a deployment explicitly opts out; the gate
remains available for deployments that want to ignore browser hints entirely.

**Alternative considered:** exposing the header through `Context`. Rejected — `Context` is a bare
key/value store today; adding request-plumbing to it is a larger change than a two-line stash in
one interceptor.

### D6. `PrincipalLocaleProvider` in `judo-runtime-core-dispatcher`

Add `PrincipalLocaleProvider implements hu.blackbelt.osgi.i18n.api.LocaleProvider` in
`judo-runtime-core-dispatcher`. It reads the resolved locale via
`PrincipalVariableProvider.apply(principalLocaleAttribute)` and returns a parsed `java.util.Locale`
(or falls back to `defaultLanguage` when the principal is absent / attribute unset). Guice and
Spring bind this to the `LocaleProvider` type so `I18nServiceImpl`'s optional `@Reference`
resolves at runtime.

**Rationale:** this is the smallest change that turns backend messages i18n-aware in every
deployment using the new params. No change to `I18nServiceImpl` itself.

**Scope note:** the impl and Guice/Spring registration ship here; the OSGi `@Component` service
registration in judo-platform is deferred (D7).

### D7. Scope = `judo-runtime-core` modules only

The judo-platform OSGi wiring (`@AttributeDefinition` mappings for the four `JUDO_PLATFORM_*` env
vars, PIDS entries in the dispatcher activator, `LocaleProvider` OSGi service registration) is
**explicitly deferred** to a follow-up change. Deployments can configure the four parameters via
Guice config or Spring properties (`judo.platform.*`) in the meantime.

**Rationale:** the runtime-core work is self-contained and testable in isolation. Splitting the
platform wiring off keeps the PR reviewable and matches how `add-acceptable-clients-whitelist`
was scoped.

## Risks / Trade-offs

- **Silent no-op on misconfigured attribute name** → Mitigated by a one-time WARN at first refresh
  (D3a) naming the offending attribute; authentication continues so the mistake is visible in
  logs without breaking users out.
- **DAO update failure** → Wrapped in `try/catch(WARN)` inside `getActorByClaims`; the actor
  payload is still returned so the request proceeds. Repeated failures surface as recurring WARN
  entries.
- **RFC-4647 lookup surprises** → `hu` matches `hu-HU` but not `hu-Latn`. Test coverage in the
  resolver includes region-only, language-only, and script-tagged inputs to lock the semantics.
- **Header spoofing via `Accept-Language`** → Only relevant when `browserLanguageCheck=true`. The
  supported-set filter (RFC-4647 against `supportedLanguages`) means an attacker can at worst
  force one of the deployment's already-supported locales; no injection surface.
- **`transient` locale attribute chosen by mistake** → D3a catches it at first login (WARN, no
  write). Document clearly in the parameter description.
- **First-login ordering** → On the very first login of a new user, `stored` is empty, so the
  resolver picks the first non-empty tier above it. This is intentional — feature-on deployments
  want the user's OIDC / browser locale to seed the DB.
- **Interceptor coupling** → `Accept-Language` capture lives only in `KeycloakLoginInterceptor`
  (D5). Non-Keycloak auth paths would need their own capture; not a concern today (Keycloak is
  the only production interceptor).
- **Precedence change is a breaking behaviour change** → Documented explicitly. Once released,
  changing the order requires a versioned migration; the fixed order (D3) is a deliberate
  commitment.

## Migration Plan

- **Deploy:** ship the runtime-core changes; existing deployments see no behaviour change
  (`principalLocaleAttribute` unset). Deployments opting in set the four parameters via Guice /
  Spring; the follow-up platform change adds env-var wiring.
- **Rollback:** unset `principalLocaleAttribute` — feature returns to today's behaviour (no
  header capture, no DB refresh, no `LocaleProvider` effect). Existing stored locale values
  remain in the DB and are simply ignored.
- **Backfill:** none needed. Users whose row has no locale get one written at first login when
  the resolver produces a value different from `null`.

## Open Questions

None blocking. All D1–D7 decisions signed off in `PRINCIPAL-LOCALE-RESOLUTION.md`. The follow-up
change will address judo-platform OSGi wiring and the OSGi `@Component` registration of
`PrincipalLocaleProvider`.
