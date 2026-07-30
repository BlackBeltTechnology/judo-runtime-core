# JNG-6415 — Multi-language Support (Principal Locale Resolution)

> **Branch:** `feature/JNG-6415_MultiLanguageSupport`
> **Modules:** `judo-runtime-core-security`, `judo-runtime-core-security-keycloak-cxf`,
> `judo-runtime-core-dispatcher`, `judo-runtime-core-guice`, `judo-runtime-core-spring`
> **Status:** Implementation complete (runtime-core only); reactor green under Java 21
> except Docker-gated PostgreSQL TestContainers tests (environmental).
> **OpenSpec change:** `add-principal-locale-resolution`.

This document is the canonical narrative of the behavioural change introduced by this
branch. For machine-checkable requirements see
`openspec/changes/add-principal-locale-resolution/specs/principal-locale-resolution/spec.md`;
for the design rationale (decisions D1–D7) see
`openspec/changes/add-principal-locale-resolution/design.md`.

---

## 1. What this feature does

On **every login**, when the authenticated user is **found in the JUDO database**, the runtime:

1. Resolves the user's effective BCP-47 locale through a fixed precedence
   (**browser → claim → stored → default**), filtered against the deployment's declared
   supported languages (RFC-4647 filtering).
2. **Persists** the resolved value back to the user's mapped locale attribute via the existing
   `DAO.update(...)` — but only when it differs from the stored value.
3. **Surfaces** the resolved locale to backend message i18n through a runtime-bound
   `LocaleProvider`, closing the long-standing gap where `I18nServiceImpl`'s optional reference
   was unbound and backend messages always fell back to the configured / JVM locale.

The feature is **model-free** (no ecore / ESM / PSM / tatami changes, no `@Locale` annotation,
no Keycloak write-back). The `principalLocaleAttribute` parameter gates the **authenticated**
paths only — the login-time refresh and the authenticated tier walk in `PrincipalLocaleProvider`.
Anonymous request locale handling (see § 10, the `add-anonymous-request-locale` change) resolves
independently from `supportedLanguages` + captured `Accept-Language`; setting
`principalLocaleAttribute` is not required for anonymous requests to see the browser locale.

## 2. What this feature does NOT do

- It does **not** produce translations. The German/Hungarian/etc. message strings come from
  `*_de.properties` / `*_hu.properties` ResourceBundle files on the backend classpath (generated
  per-application by tatami from the model) and from the frontend's own JSON i18n files. This
  feature only decides *which* locale is active and propagates it.
- It does **not** add judo-platform OSGi wiring (`@AttributeDefinition` + PIDS mapping the
  `JUDO_PLATFORM_*` env vars, and the OSGi `@Component` registration of `PrincipalLocaleProvider`).
  That is a deferred follow-up; meanwhile the parameters are configurable via Guice config and
  Spring `judo.platform.*` properties.

## 3. Parameters

| Env var (platform, deferred) | Guice config | Spring property | Meaning | Default |
|---|---|---|---|---|
| `JUDO_PLATFORM_PRINCIPAL_LOCALE_ATTRIBUTE` | `actorResolverPrincipalLocaleAttribute` | `judo.platform.principalLocaleAttribute` | mapped actor attribute holding the BCP-47 locale | unset ⇒ **feature OFF** |
| `JUDO_PLATFORM_SUPPORTED_LANGUAGES` | `actorResolverSupportedLanguages` | `judo.platform.supportedLanguages` | comma list of offered tags (RFC-4647 filtered) | empty (only default resolves) |
| `JUDO_PLATFORM_DEFAULT_LANGUAGE` | `actorResolverDefaultLanguage` | `judo.platform.defaultLanguage` | terminal fallback | `en-US` |
| `JUDO_PLATFORM_LOCALE_RESOLUTION_LEVEL` | `actorResolverLocaleResolutionLevel` | `judo.platform.localeResolutionLevel` | ceiling naming the highest tier the runtime consults — one of `BROWSER` / `IDENTITY_PROVIDER` / `PRINCIPAL` (see § 4.a) | **`BROWSER`** |

## 4. Precedence (fixed order)

| # | Tier | Source | Notes |
|---|------|--------|-------|
| 1 | `browser` | request `Accept-Language` (captured at the Keycloak auth interceptor), quality-ordered | only when `localeResolutionLevel.includes(BROWSER)` (the default) |
| 2 | `claim` | OIDC `locale` claim | |
| 3 | `stored` | current DB value of the locale attribute | |
| 4 | `default` | `defaultLanguage` → `en-US` | terminal; always resolves |

Each of tiers 1–3 is filtered against `supportedLanguages` using RFC-4647 **filtering**
(`Locale.filterTags`): a broad range such as `hu` matches a specific supported tag such as
`hu-HU`; `en-GB` does **not** match `en-US`. A candidate that matches nothing is skipped so the
next tier is consulted. The order is fixed — there is no reorder knob (design D3).

### 4.a `LocaleResolutionLevel` ceiling

The `localeResolutionLevel` parameter is a **ceiling** — it names the highest-priority tier the
runtime is allowed to consult; every lower tier stays active; `default` is always terminal. The
ceiling is honoured uniformly by the authenticated tier walk, the Keycloak `Accept-Language`
capture (`KeycloakLoginInterceptor`), and the anonymous request branch of
`PrincipalLocaleProvider`.

| Level | Browser (`Accept-Language`) | Identity provider (claim) | Principal (stored) |
|---|:---:|:---:|:---:|
| `BROWSER` *(default)* | ✔ | ✔ | ✔ |
| `IDENTITY_PROVIDER` | ✘ | ✔ | ✔ |
| `PRINCIPAL` | ✘ | ✘ | ✔ |

Use `IDENTITY_PROVIDER` for corporate SSO deployments where the IdP resolves the user's language
and the raw browser hint is untrusted. Use `PRINCIPAL` to lock resolution to the stored value
only (e.g. tests, or deployments where the user's preference is authoritative and neither the
browser nor the claim should override it).

Replaces the boolean `browserLanguageCheck` introduced by JNG-6415 by the
`replace-browser-check-with-resolution-level` change (branch-local rename, no back-compat alias
since JNG-6415 is unreleased): `true → BROWSER`, `false → IDENTITY_PROVIDER`.

## 5. Key classes

| Class | Module | Role |
|---|---|---|
| `LocaleResolutionLevel` | security | Ceiling enum with `PRINCIPAL < IDENTITY_PROVIDER < BROWSER`. `DEFAULT = BROWSER`. `includes(tier)` returns `ordinal() >= tier.ordinal()`. `parse(String)` is lenient (case-insensitive, null / blank / unknown → `DEFAULT`, never throws). |
| `PrincipalLocaleResolver` | security | Pure helper. Three static methods: `resolve(browserHint, claim, stored, default, supported, level)` (full tier walker), `matchSupportedLanguage(candidate, supported)` (single-tier RFC-4647 filter used by the anonymous branch), and `parseSupportedLanguages(csv)` (memoised once by `PrincipalLocaleConfig`). Also defines `ACCEPT_LANGUAGE_ATTRIBUTE = "__acceptLanguage"`. |
| `PrincipalLocaleConfig` | security | Immutable value object grouping the four locale settings into one bundle carried through the runtime's Guice/Spring wiring. `@Value @Builder`, memoized `getParsedSupportedLanguages()`, `isEnabled()` gate. |
| `KeycloakLoginInterceptor` | security-keycloak-cxf | Captures the request `Accept-Language` header into `attributes["__acceptLanguage"]` when the deployment's `LocaleResolutionLevel` ceiling `includes(BROWSER)` (i.e. `BROWSER`, or `null` at construction time). Internally holds a boolean `captureBrowserLanguage` derived once from the enum; the static helper `captureAcceptLanguage(attributes, request, boolean)` keeps its boolean signature. |
| `DefaultActorResolver` | dispatcher | `refreshActorLocale(...)` glue in `getActorByClaims`; pure `computeLocaleRefresh(...)` decision; safe `applyLocaleRefresh(...)` write (try/catch WARN). D3a mapped-attribute check with one WARN per actor type. Takes a `PrincipalLocaleConfig` on the builder. |
| `PrincipalLocaleProvider` | dispatcher | `implements LocaleProvider`; cheap O(1) read of the resolved locale from the request `Context` (`ACTOR_KEY` then `PRINCIPAL_KEY`), fallback to `defaultLanguage`. Takes a `PrincipalLocaleConfig` on the ctor. Bound in Guice + Spring. |

### 5.a Configuration surface

The four parameters in § 3 are exposed to the app **unchanged** — same env-var names, same
`judo.platform.*` property names, same defaults, same `JudoDefaultModuleConfiguration.builder()`
fields. Internally, since the `consolidate-locale-config-object` change, they flow through the
runtime as a **single value object** (`PrincipalLocaleConfig`) rather than four independent scalar
bindings. The four Guice `@BindingAnnotation` qualifiers that JNG-6415 originally introduced
(`ActorResolver{PrincipalLocaleAttribute,SupportedLanguages,DefaultLanguage,BrowserLanguageCheck}`)
have been **removed** — embedders no longer bind them individually; they configure the settings
via `JudoDefaultModuleConfiguration` (Guice) or `judo.platform.*` properties (Spring) exactly as
before, and a single `PrincipalLocaleConfig` binding is derived once per assembly.

## 6. Behavioural guarantees

- **Authenticated feature-off default:** with `principalLocaleAttribute` unset there is no header
  capture at the Keycloak login interceptor, no DB refresh, and the authenticated tier walk in
  `PrincipalLocaleProvider` short-circuits. The anonymous request branch is unaffected — with
  `supportedLanguages` set and a captured `Accept-Language`, `PrincipalLocaleProvider` still
  resolves for anonymous requests.
- **Mapped-attribute requirement (D3a):** if `principalLocaleAttribute` does not name a mapped,
  non-`transient` attribute, `refreshActorLocale` skips the `DAO.update` write and logs a single
  WARN per actor type (once per JVM lifetime). The in-request actor payload is left untouched —
  whatever value the DAO returned (including `null`) stays as-is; the runtime does **not** force
  `defaultLanguage` into the payload. `PrincipalLocaleProvider` computes the effective locale
  from the untouched payload via the usual tier walk. Authentication is unaffected.
- **Refresh never breaks auth:** the `dao.update` is wrapped in try/catch(WARN); a failed write
  returns the actor payload as if no refresh had been attempted.
- **Browser tier is ceiling-gated:** on by default (`localeResolutionLevel=BROWSER`); set
  `localeResolutionLevel=IDENTITY_PROVIDER` to skip only the browser tier while still honouring
  the OIDC claim, or `PRINCIPAL` to skip both browser and claim (stored value only). The ceiling
  applies uniformly to the authenticated tier walk, the Keycloak `Accept-Language` capture, and
  the anonymous request branch of `PrincipalLocaleProvider`.

### 6.a Wiring-gap fixes bundled with the enum swap

The `replace-browser-check-with-resolution-level` change also fixes two latent gaps in how the
toggle propagated:

- **Guice–Keycloak provider gap:** `KeycloakLoginInterceptorProvider`
  (`judo-runtime-core-guice-keycloak`) previously never set the toggle on the interceptor — the
  default-on boolean was silently in effect under Guice, and `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK=false`
  had no effect on capture. Now the provider injects `PrincipalLocaleConfig` (optional) and
  threads `localeResolutionLevel` into the interceptor builder. Spring builds its own interceptor
  and was unaffected.
- **Anonymous-branch inconsistency:** `PrincipalLocaleProvider.getLocale()`'s anonymous branch
  previously read `RequestLocaleHolder` unconditionally, so the toggle was ignored for
  anonymous requests. Now the anonymous browser-tier read is gated by
  `localeResolutionLevel.includes(BROWSER)`, matching the authenticated tier walk.

## 7. Design note — why `PrincipalLocaleProvider` reads from `Context`

The original OpenSpec task proposed reading the locale via
`PrincipalVariableProvider.apply(principalLocaleAttribute)`. That call triggers a full
`GET_PRINCIPAL` operation dispatch, and `I18nServiceImpl` invokes the locale supplier on **every
message-key lookup** — so that path would be catastrophically expensive and risk recursion during
error formatting. The implementation instead reads the resolved locale directly from the
request-scoped `Context` (the loaded actor payload under `ACTOR_KEY`, then the principal's token
attributes under `PRINCIPAL_KEY`), which is O(1) and side-effect-free. `DefaultActorResolver`
writes the resolved value into the in-request actor payload so the provider (and
`getPrincipal()`) reflect it immediately — no DB write in this branch per the JNG-6415 review
"backend reads only" decision; that persistence is deferred to a follow-up capability.

## 8. Configuration examples

**Spring** (`application.properties`):
```properties
judo.platform.principalLocaleAttribute=locale
judo.platform.supportedLanguages=en-US,hu-HU,de-DE
judo.platform.defaultLanguage=en-US
judo.platform.localeResolutionLevel=BROWSER
```

**Guice** (`JudoDefaultModule.builder()` / `JudoDefaultModuleConfiguration.builder()`):
```java
JudoDefaultModuleConfiguration.builder()
    .actorResolverPrincipalLocaleAttribute("locale")
    .actorResolverSupportedLanguages("en-US,hu-HU,de-DE")
    .actorResolverDefaultLanguage("en-US")
    .actorResolverLocaleResolutionLevel(LocaleResolutionLevel.BROWSER)
    // ...
    .build();
```

## 9. Testing

- `LocaleResolutionLevelTest` (security) — 10 tests: `includes(...)` truth table (3 cases) +
  `parse(...)` lenient parsing (7 cases: exact, mixed case, whitespace, null, blank, garbage,
  `DEFAULT` identity).
- `PrincipalLocaleResolverTest` (security) — 36 `@Test` methods across seven nested groups
  (BrowserTier, ClaimTier, StoredTier, DefaultTier, SupportedLanguages, ResolutionLevelCeiling,
  Purity, ParseSupportedLanguages): CSV parsing, RFC-4647 filtering, tier order, browser tier
  gating, fall-through, malformed/blank inputs, terminal default, referential transparency, plus
  the new "resolution level ceiling" group (6 tests) covering `PRINCIPAL` and `IDENTITY_PROVIDER`
  behaviours and null-level-defaults-to-`BROWSER`.
- `PrincipalLocaleConfigTest` (security) — asserts the default builder exposes
  `localeResolutionLevel=BROWSER`, explicit `PRINCIPAL` / `IDENTITY_PROVIDER` are preserved, and
  value-object equality/hashCode round-trip through every field including the enum.
- `KeycloakLoginInterceptorAcceptLanguageTest` (security-keycloak-cxf) — 6 tests: gate on/off,
  header present/absent/blank, null request, raw-not-parsed. Unchanged by the enum swap because
  the static `captureAcceptLanguage(..., boolean)` helper's signature is preserved (D6 in the
  change design).
- `DefaultActorResolverLocaleRefreshTest` (dispatcher) — 12 tests: decision
  (`computeLocaleRefresh`) and safe write (`applyLocaleRefresh`, now no-op per the "backend reads
  only" review). The `cfg(attr, browserCheck)` helper maps the boolean to
  `LocaleResolutionLevel.BROWSER` / `.IDENTITY_PROVIDER` so historical scenarios survive the
  enum swap unchanged.
- `DefaultActorResolverLocaleRefreshModelTest` (dispatcher) — 5 tests against a real synthetic
  ASM model, covering D3a introspection with the enum ceiling.
- `PrincipalLocaleProviderTest` (dispatcher) — **18 `@Test` methods total** (originally 8 for the
  authenticated branch; extended to 15 by `add-anonymous-request-locale` with 7 anonymous-branch
  cases; further extended to 18 by this change with 3 anonymous-ceiling cases covering `BROWSER`
  / `IDENTITY_PROVIDER` / `PRINCIPAL` behaviour in the anonymous branch).
- `JudoDefaultHsqldbModuleTest` — exercises full Guice injector creation including the
  `LocaleProvider` binding and the fixed `KeycloakLoginInterceptorProvider` locale wiring.

The ASM-model introspection glue in `refreshActorLocale` (exists/mapped/transient → `persistable`)
uses `AsmUtils` instance methods that cannot be mocked; it is left for the real-model e2e / platform
round.

## 10. Anonymous requests (OpenSpec change `add-anonymous-request-locale`)

The core JNG-6415 feature only applies to authenticated users (it needs a principal + DB row).
A companion change extends locale support to **anonymous** requests — a request-scoped locale for
backend message i18n, **with no persistence** (there is no user row to write to).

- **Generalized provider:** `PrincipalLocaleProvider` now serves both cases. When a principal is
  bound it behaves exactly as above (actor → principal → default). When **no principal** is bound it
  resolves the request `Accept-Language` against `supportedLanguages` (RFC-4647 via
  `PrincipalLocaleResolver.matchSupportedLanguage`), after honoring any application-set request
  `Locale` (`LOCALE_KEY`), then falls back to `defaultLanguage`.
- **Capture:** `AcceptLanguageCaptureInterceptor` (CXF `AbstractPhaseInterceptor`, RECEIVE phase, in
  `judo-runtime-core-jaxrs-cxf`) reads the header and stores it in `RequestLocaleHolder` — a
  thread-local in the core module.
- **Why a thread-local, not the dispatcher `Context`:** `DefaultDispatcher.callOperation` calls
  `context.removeAll()` for exposed (HTTP-entry) operations and repopulates `LOCALE_KEY`/`ACTOR_KEY`
  from the exchange, so a value stashed in `Context` by a JAX-RS filter would be wiped before
  messages format. The exchange itself is built by the platform/generated REST layer, not
  runtime-core. A thread-local set by the transport interceptor survives that reset. The interceptor
  sets the holder on every request (clearing when the header is absent) so a pooled thread never
  keeps a stale value.
- **Wiring:** Guice registers the interceptor via `JudoCxfModule` and passes `supportedLanguages` to
  the provider. Spring passes `supportedLanguages` to the `LocaleProvider` bean; the interceptor
  registration for Spring is a Spring Boot starter concern (runtime-core-spring has no CXF server).
- **Frontend synergy:** the frontend already sends `Accept-Language` from its active locale, so a
  frontend language switch drives anonymous backend messages with no further work.

## 11. Deferred / follow-up

- judo-platform OSGi wiring: `@AttributeDefinition`s for the four `JUDO_PLATFORM_*` env vars, PIDS
  entries in the dispatcher/security activator, and the OSGi `@Component` service registration of
  `PrincipalLocaleProvider` so it binds to `I18nServiceImpl`'s `@Reference` in OSGi runtimes.
- End-to-end verification against a running app with Keycloak + a model that ships `hu`/`de`
  translation bundles.
- Frontend template work (out of scope; tracked separately).
- Spring Boot starter: register `AcceptLanguageCaptureInterceptor` in its CXF server assembly.
- judo-platform OSGi: CXF interceptor service registration (shared with the OSGi wiring follow-up).
