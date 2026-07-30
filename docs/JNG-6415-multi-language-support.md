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

On **every backend message-formatting call**, when an authenticated principal is bound to the
request, the runtime:

1. Resolves the user's effective BCP-47 locale through a fixed precedence
   (**browser → claim → stored → default**), filtered against the deployment's declared
   supported languages (RFC-4647 filtering) — **purely as a read** over the request-scoped
   context. Nothing is written.
2. **Surfaces** the resolved locale to backend message i18n through a runtime-bound
   `LocaleProvider`, closing the long-standing gap where `I18nServiceImpl`'s optional reference
   was unbound and backend messages always fell back to the configured / JVM locale.

The feature is **model-free** (no ecore / ESM / PSM / tatami changes, no `@Locale` annotation,
no Keycloak write-back) and **off by default** — it activates only when
`principalLocaleAttribute` is configured.

## 2. What this feature does NOT do

- It does **not persist** the user's preferred language. There is no `DAO.update` call, no
  mutation of the in-memory actor payload, and no Keycloak profile update. The stored locale is
  read only. Persisting a user-selected language is a separate capability, tracked as a follow-up.
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
| `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK` | `actorResolverBrowserLanguageCheck` | `judo.platform.browserLanguageCheck` | when `true`, `Accept-Language` is the top tier | **`true`** |

## 4. Precedence (fixed order)

| # | Tier | Source | Notes |
|---|------|--------|-------|
| 1 | `browser` | request `Accept-Language` (captured at the Keycloak auth interceptor), quality-ordered | only when `browserLanguageCheck=true` |
| 2 | `claim` | OIDC `locale` claim | |
| 3 | `stored` | current DB value of the locale attribute | |
| 4 | `default` | `defaultLanguage` → `en-US` | terminal; always resolves |

Each of tiers 1–3 is filtered against `supportedLanguages` using RFC-4647 **filtering**
(`Locale.filterTags`): a broad range such as `hu` matches a specific supported tag such as
`hu-HU`; `en-GB` does **not** match `en-US`. A candidate that matches nothing is skipped so the
next tier is consulted. The order is fixed — there is no reorder knob (design D3).

## 5. Key classes

| Class | Module | Role |
|---|---|---|
| `PrincipalLocaleResolver` | security | Pure helper: `parseSupportedLanguages(csv)`, `resolve(browser, claim, stored, default, supported, browserGate)` (full walk with terminal default), and `matchSupportedLanguage(candidate, supported)` (single-tier RFC-4647 filter). Also defines `ACCEPT_LANGUAGE_ATTRIBUTE = "__acceptLanguage"`. |
| `PrincipalLocaleConfig` | security | Immutable value object grouping the four locale settings into one bundle carried through the runtime's Guice/Spring wiring. `@Value @Builder`, memoized `getParsedSupportedLanguages()`, `isEnabled()` gate. |
| `KeycloakLoginInterceptor` | security-keycloak-cxf | Captures the request `Accept-Language` header into `attributes["__acceptLanguage"]` when `browserLanguageCheck=true` (new `captureAcceptLanguage(...)` helper). |
| `DefaultActorResolver` | dispatcher | **Untouched by JNG-6415** — no locale fields, no builder params, no logic. Locale resolution happens at read time inside `PrincipalLocaleProvider`. |
| `PrincipalLocaleProvider` | dispatcher | `implements LocaleProvider`. On each call, when a principal is bound, walks the tier order over the request-scoped `Context` (browser hint from `JudoPrincipal.attributes[__acceptLanguage]`, claim from `JudoPrincipal.attributes[localeAttr]`, stored value from `Context[ACTOR_KEY][localeAttr]`), falls back to `defaultLanguage`. When no principal is bound, handles the anonymous path (`LOCALE_KEY` → `RequestLocaleHolder` → default). Takes a `PrincipalLocaleConfig` on the ctor. Bound in Guice + Spring. |

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

- **Backend is read-only.** No `DAO.update`, no mutation of the in-memory actor payload, no
  Keycloak write-back. The stored locale is read; nothing is written back.
- **Feature-off default:** with `principalLocaleAttribute` unset there is no header capture and
  `PrincipalLocaleProvider` returns the default (identical to an unbound provider).
- **Resolution never breaks anything:** the tier walk catches `RuntimeException` internally and
  falls back to `defaultLanguage`; a malformed input at any tier is skipped, not fatal.
- **Browser tier is opt-out:** on by default; set `browserLanguageCheck=false` to disable both the
  header capture and the browser tier.

## 7. Design note — why `PrincipalLocaleProvider` reads from `Context`

The original OpenSpec task proposed reading the locale via
`PrincipalVariableProvider.apply(principalLocaleAttribute)`. That call triggers a full
`GET_PRINCIPAL` operation dispatch, and `I18nServiceImpl` invokes the locale supplier on **every
message-key lookup** — so that path would be catastrophically expensive and risk recursion during
error formatting. The implementation instead reads the three candidate inputs directly from the
request-scoped `Context` (browser hint on principal attributes, OIDC claim on principal attributes,
stored value on the loaded actor payload) and lets `PrincipalLocaleResolver` pick one against the
supported set. Every operation is a cheap string op against an in-memory context; there is no DAO
call, no dispatch, and no state to mutate.

## 8. Configuration examples

**Spring** (`application.properties`):
```properties
judo.platform.principalLocaleAttribute=locale
judo.platform.supportedLanguages=en-US,hu-HU,de-DE
judo.platform.defaultLanguage=en-US
judo.platform.browserLanguageCheck=true
```

**Guice** (`JudoDefaultModule.builder()` / `JudoDefaultModuleConfiguration.builder()`):
```java
JudoDefaultModuleConfiguration.builder()
    .actorResolverPrincipalLocaleAttribute("locale")
    .actorResolverSupportedLanguages("en-US,hu-HU,de-DE")
    .actorResolverDefaultLanguage("en-US")
    .actorResolverBrowserLanguageCheck(true)
    // ...
    .build();
```

## 9. Testing

- `PrincipalLocaleResolverTest` (security) — 38 tests: CSV parsing, RFC-4647 filtering, tier order,
  browser gate, fall-through, malformed/blank inputs, terminal default, referential transparency,
  and the single-tier `matchSupportedLanguage` helper.
- `KeycloakLoginInterceptorAcceptLanguageTest` (security-keycloak-cxf) — 6 tests: gate on/off,
  header present/absent/blank, null request, raw-not-parsed.
- `PrincipalLocaleProviderTest` (dispatcher) — 21 tests: authenticated tier walk (browser → claim
  → stored → default, incl. `browserLanguageCheck=false` skips browser), unsupported stored value
  falls through, feature-off, blank / malformed / underscored value, principal-only-no-actor,
  anonymous path (`LOCALE_KEY` → `RequestLocaleHolder` → default), null-context safety, and a
  regression guard asserting the actor payload is not mutated by resolution.
- `JudoDefaultHsqldbModuleTest` — exercises full Guice injector creation including the
  `LocaleProvider` binding.

The login-write test classes (`DefaultActorResolverLocaleRefreshTest`,
`DefaultActorResolverLocaleRefreshModelTest`) were deleted along with the login-time refresh
logic they covered.

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

- **Persistence of the user's preferred language.** A separate capability will handle writing the
  resolved / user-selected locale back to the DB (or Keycloak), including the frontend surface
  and read-after-write consistency across sessions. Explicitly out of scope for this change.
- judo-platform OSGi wiring: `@AttributeDefinition`s for the four `JUDO_PLATFORM_*` env vars, PIDS
  entries in the dispatcher/security activator, and the OSGi `@Component` service registration of
  `PrincipalLocaleProvider` so it binds to `I18nServiceImpl`'s `@Reference` in OSGi runtimes.
- End-to-end verification against a running app with Keycloak + a model that ships `hu`/`de`
  translation bundles.
- Frontend template work (out of scope; tracked separately).
- Spring Boot starter: register `AcceptLanguageCaptureInterceptor` in its CXF server assembly.
- judo-platform OSGi: CXF interceptor service registration (shared with the OSGi wiring follow-up).
