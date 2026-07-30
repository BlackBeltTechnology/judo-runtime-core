## Context

Two independent facts, verified against the live tree, motivate the design:

1. **The login choke-point** `DefaultActorResolver.authenticateActor(exchange)` →
   `authenticateByPrincipal` → `getActorByClaims` is the single point where the actor entity is
   loaded from the JUDO DB (`dao.search(actorType, …)`). This is where the stored locale becomes
   readable off the actor payload for the rest of the request. The backend does not write it back.
2. **`Accept-Language` is only reachable at the auth interceptor.** `KeycloakLoginInterceptor`
   holds the `HttpServletRequest` (`message.get("HTTP.REQUEST")`). Downstream, `Context` is a bare
   key/value store — no request access. Any header signal must be captured at the interceptor and
   ridden into `JudoPrincipal.attributes` (String-valued, to survive the String-only claims filter
   at `DefaultActorResolver.java:110-113`).

Additional context:

- **Principal attributes are split.** Mapped attributes live in the JUDO RDBMS (readable via
  `dao.search`/`getByIdentifier`). `transient`-annotated attributes are copied from Keycloak
  claims on every request. `getPrincipal()` returns the merge. The resolver reads from both but
  writes to neither.
- **`LocaleProvider` has no runtime implementation today.** The interface exists
  (`hu.blackbelt.osgi.i18n.api.LocaleProvider`) and is consumed as an optional `@Reference` by
  `I18nServiceImpl`, `EnumI18nServiceImpl`, and the Hibernate `MessageInterpolator`, but no
  runtime-core class implements it. Backend i18n therefore falls back to `defaultLocale` / JVM in
  every deployment.
- **Wiring pattern is established.** The proven flow used for `checkMappedActors` and
  `acceptableClients` (env var → OSGi `@AttributeDefinition` + PIDS → activator config →
  Guice/Spring binding) is copied for the four new parameters, grouped into a single
  `PrincipalLocaleConfig` value object so the Guice/Spring wiring reduces to one binding. Only the
  runtime-core layers (Guice + Spring binding + `LocaleProvider` binding) are in scope for this
  change; the judo-platform OSGi wiring is a follow-up.

## Goals / Non-Goals

**Goals:**
- Resolve the authenticated user's effective BCP-47 locale via a fixed, documented precedence
  (browser → claim → stored → default) purely as a **read** over the request-scoped context.
- Filter every candidate against a deployment-declared `supportedLanguages` set using RFC-4647
  lookup, so an unsupported locale is never returned.
- Close the `LocaleProvider`-unbound gap so backend i18n messages follow the resolved locale.
- Keep the precedence logic pure and unit-testable, isolated from I/O and DAO calls.
- Zero behavioural change when `principalLocaleAttribute` is unset (feature-off default).
- No metamodel changes; no `judo-dispatcher-api` changes; no new DAO surface.
- **No mutation** anywhere: no `DAO.update`, no in-memory actor-payload edit, no Keycloak
  write-back. The backend is a pure reader of locale inputs.

**Non-Goals:**
- Persisting the user's preferred language back to the DB / OIDC. Deferred to a later capability
  that handles the full "user updates preference" story (including the frontend surface and the
  read-after-write consistency across sessions).
- Configurable precedence order (`languageOrder` env var). Fixed order this round; adding it later
  is a self-contained follow-up.
- Keycloak write-back (updating the user's Keycloak profile locale).
- judo-platform OSGi wiring (`@AttributeDefinition` + PIDS, `LocaleProvider` OSGi service
  registration). Deferred to a follow-up change.
- Metamodel-level `@Locale` annotation, `SET_PRINCIPAL_LOCALE` behaviour, PSM/ESM/ecore edits,
  tatami transforms.
- Frontend template changes; anything under `judo-ui-*`.
- Migration/backfill of existing DB rows (feature is opt-in; the resolver simply reads what is
  there and falls back through the tiers).

## Decisions

### D1. Resolve at read time inside `PrincipalLocaleProvider`, not at login inside `DefaultActorResolver`

The full RFC-4647 tier walk (browser → claim → stored → default) happens on each
`PrincipalLocaleProvider.getLocale()` call. Reads target the request-scoped `Context`: browser
hint from `JudoPrincipal.attributes["__acceptLanguage"]`, OIDC `locale` claim from
`JudoPrincipal.attributes[principalLocaleAttribute]`, stored value from
`Context[ACTOR_KEY][principalLocaleAttribute]`. `DefaultActorResolver` is not touched by
JNG-6415.

**Rationale:** with persistence removed as an explicit non-goal (see above), there is nothing
special about "login time" for locale resolution — the same three inputs are available on every
request. Concentrating the logic in the provider keeps `DefaultActorResolver` free of any locale
concern, avoids ASM-model introspection (no "is the attribute mapped?" check needed when nothing
is written), and makes the feature purely additive on the read path. Reads are string operations
against an in-memory context — cheap, deterministic, and independent of DAO state.

**Alternative considered (rejected):** resolve once at login and stash the answer into the actor
payload or a `LOCALE_KEY` context slot. This still edits state the backend has no business
editing, and the "read from Context" implementation ends up identical anyway.

### D2. Backend is read-only (no persistence in this change)

No `DAO.update` call, no `result.put(localeAttr, resolved)` mutation of the in-memory actor
payload, no Keycloak profile update. The stored locale is read exactly as it sits in the DB and
combined with the other tiers to pick an effective value for message formatting only.

**Rationale:** persisting the user's preferred language is a bigger story — it belongs to a
user-driven "change my language" action, not to a login-time reconciliation. Removing the write
also removes an entire class of edge cases (unmapped/transient attribute detection, write-failure
handling, one-time-warning bookkeeping, "first login seeds DB" semantics) and shrinks
`DefaultActorResolver` back to its pre-JNG-6415 shape.

### D3. Pure helper with fixed order + `browserLanguageCheck` boolean

`PrincipalLocaleResolver` lives in `judo-runtime-core-security` (next to `AcceptableClientsParser`)
as a pure class — no DAO, no logger side-effects, no request access. It exposes `resolve(...)`
(full tier walk with terminal default) and `matchSupportedLanguage(...)` (single-tier RFC-4647
filter, used both by the anonymous branch and by the authenticated tier walk in
`PrincipalLocaleProvider`). The tier order is hard-coded: `browser` (only if gate true) → `claim`
→ `stored` → `default`. RFC-4647 lookup is done per tier; unsupported tags fall through.

**Rationale:** the helper is trivially unit-testable, has no framework dependency, and is small
enough that swapping in a configurable order later is a bounded change (see D3-alt). Fixed order
also avoids a whole class of misconfiguration bugs.

**Alternative considered (D3-alt):** a `languageOrder` env var. Rejected for this round to keep
the surface minimal.

### D5. `Accept-Language` capture in `KeycloakLoginInterceptor`

When `browserLanguageCheck=true` (the default), `KeycloakLoginInterceptor.handleMessage` reads
the request's `Accept-Language` header and puts it into `attributes["__acceptLanguage"]` before
building the `JudoPrincipal`. The double-underscore prefix marks it as a runtime-internal key
that will never collide with a Keycloak claim attribute (Keycloak claim names never begin with
`__`). Setting the gate to `false` disables the read entirely.

**Rationale:** this is the only reachable point that holds the `HttpServletRequest`, and stashing
into `attributes` is the only route that survives the String-only claims filter in
`DefaultActorResolver` (L110-113). Default-on matches the pragmatic expectation that a user's
browser preference should influence locale unless a deployment explicitly opts out.

**Alternative considered:** exposing the header through `Context`. Rejected — `Context` is a bare
key/value store today; adding request-plumbing to it is a larger change than a two-line stash in
one interceptor. (For the anonymous request path, the sibling `add-anonymous-request-locale`
change uses a thread-local holder set by a CXF interceptor, since no `JudoPrincipal` exists.)

### D6. `PrincipalLocaleProvider` in `judo-runtime-core-dispatcher`

Add `PrincipalLocaleProvider implements hu.blackbelt.osgi.i18n.api.LocaleProvider` in
`judo-runtime-core-dispatcher`. On each call it walks the tier order over `Context` and returns a
parsed `java.util.Locale` (or falls back to `defaultLanguage` when no tier resolves). It reads
`Context` directly rather than invoking `PrincipalVariableProvider.apply(...)`, since the latter
would trigger a full `GET_PRINCIPAL` dispatch per message-key lookup and risk recursion during
error formatting. Guice and Spring bind this as the `LocaleProvider` so `I18nServiceImpl`'s
optional `@Reference` resolves at runtime.

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

### D8. `PrincipalLocaleConfig` value object

The four settings are grouped into a single immutable `PrincipalLocaleConfig` value object
(Lombok `@Value @Builder`) carried through Guice and Spring wiring. Both `LocaleProvider`
consumers receive it via one binding instead of four separate qualifiers / `@Value` sites, and
the parsed supported-language set is memoized on the config (Lombok `@Getter(lazy = true)`) so
the CSV is split at most once per JVM.

**Rationale:** this reduces the touch surface for adding a fifth locale setting later to ≤ 2
files (the value object plus the app-facing `JudoDefaultModuleConfiguration`), and keeps the
config parse cost off the `PrincipalLocaleProvider` construction path.

## Risks / Trade-offs

- **Resolution runs on every `getLocale()` call.** Each call does a few `context.getAs` reads plus
  RFC-4647 filtering — all in-memory string ops. No I/O, no DAO. Measured cost is negligible
  compared to the message formatting that follows.
- **RFC-4647 lookup surprises** → `hu` matches `hu-HU` but not `hu-Latn`. Test coverage in the
  resolver includes region-only, language-only, and script-tagged inputs to lock the semantics.
- **Header spoofing via `Accept-Language`** → Only relevant when `browserLanguageCheck=true`. The
  supported-set filter (RFC-4647 against `supportedLanguages`) means an attacker can at worst
  force one of the deployment's already-supported locales; no injection surface.
- **Interceptor coupling** → `Accept-Language` capture lives only in `KeycloakLoginInterceptor`
  (D5). Non-Keycloak auth paths would need their own capture; not a concern today (Keycloak is
  the only production interceptor).
- **Precedence change is a breaking behaviour change** → Documented explicitly. Once released,
  changing the order requires a versioned migration; the fixed order (D3) is a deliberate
  commitment.
- **No stored-preference persistence yet** → A user who selects a language in the frontend
  today has that persisted by the frontend's own storage (localStorage), not by the JUDO backend.
  The backend read-only path here is complementary; a follow-up capability will add persistence
  when the "user updates preference" story is designed end-to-end.

## Migration Plan

- **Deploy:** ship the runtime-core changes; existing deployments see no behaviour change
  (`principalLocaleAttribute` unset). Deployments opting in set the four parameters via Guice /
  Spring; the follow-up platform change adds env-var wiring.
- **Rollback:** unset `principalLocaleAttribute` — feature returns to today's behaviour (no
  header capture, no `LocaleProvider` effect on messages). Nothing in the DB changed, so there is
  nothing to unwind.
- **Backfill:** none needed. Existing stored locale values (if any) are simply read as the
  `stored` tier.

## Open Questions

None blocking. Persistence of the user's preferred language is deferred to a follow-up capability
and does not need to be resolved for this change to land.
