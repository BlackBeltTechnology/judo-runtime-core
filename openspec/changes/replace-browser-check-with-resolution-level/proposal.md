## Why

`JNG-6415` (`add-principal-locale-resolution` and its consolidation into `PrincipalLocaleConfig`)
gated the browser tier with a single boolean `browserLanguageCheck`. That boolean is a **two-state
switch on a three-tier precedence** (browser → identity-provider claim → principal stored), which
means the runtime cannot express the middle option "trust the identity provider but not the raw
browser hint" — a legitimate deployment posture (e.g. a corporate SSO deployment where the IdP
already resolves the user's language and the browser header is untrusted / spoofable).

The current boolean also hides three latent issues verified against the tree:

1. **Guice–Keycloak wiring gap.** `KeycloakLoginInterceptorProvider`
   (`judo-runtime-core-guice-keycloak`) never sets `browserLanguageCheck` on the interceptor at
   all — the default-on boolean is silently in effect under Guice, and the configured value under
   Guice is ignored. Deployments that opted out via `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK=false`
   would still see the header captured. (Spring builds its own interceptor and is unaffected.)
2. **Anonymous branch inconsistency.** `PrincipalLocaleProvider.getLocale()` honours the toggle for
   the authenticated tier walk but the **anonymous** branch reads `RequestLocaleHolder` and matches
   it against `supportedLanguages` unconditionally — so setting the toggle off still lets the
   anonymous browser hint drive the locale.
3. **Naming.** `browserLanguageCheck` reads as an assertion about the header itself, not as a
   *precedence ceiling*. Reviewers repeatedly asked "what if we want claim-only, no browser?" —
   the answer today is "you can't".

`JNG-6415` is unreleased (feature branch only), so we can **replace the boolean outright** — no
deprecated alias, no back-compat shim.

## What Changes

- Introduce a new enum `hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel` with three
  constants, in **ascending precedence order** so `ordinal()` corresponds to "how much the runtime
  interferes":

  | Ordinal | Constant | Browser | Claim | Stored |
  |:---:|---|:---:|:---:|:---:|
  | 0 | `PRINCIPAL` | ✘ | ✘ | ✔ |
  | 1 | `IDENTITY_PROVIDER` | ✘ | ✔ | ✔ |
  | 2 | `BROWSER` *(default)* | ✔ | ✔ | ✔ |

  A tier is enabled when `level.ordinal() >= tier.ordinal()`. `default` is always terminal. Ships
  with `includes(LocaleResolutionLevel)` (ceiling check), `parse(String)` (lenient, case-insensitive,
  null/blank/unknown → `DEFAULT = BROWSER`, never throws), and a `DEFAULT` constant.

- Swap the `PrincipalLocaleResolver.resolve(...)` last parameter from `boolean browserLanguageCheck`
  to `LocaleResolutionLevel level` and walk the tiers by ceiling
  (`level.includes(BROWSER)` gates browser; `level.includes(IDENTITY_PROVIDER)` gates claim; stored
  is unguarded — it is the floor).

- Swap `PrincipalLocaleConfig.browserLanguageCheck` (Boolean, `@Builder.Default = TRUE`) with
  `PrincipalLocaleConfig.localeResolutionLevel` (`LocaleResolutionLevel`, `@Builder.Default = BROWSER`).

- Rename the platform-level configuration knobs:

  | Old | New |
  |---|---|
  | `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK` | `JUDO_PLATFORM_LOCALE_RESOLUTION_LEVEL` |
  | `judo.platform.browserLanguageCheck` (Spring) | `judo.platform.localeResolutionLevel` |
  | `actorResolverBrowserLanguageCheck` (Guice `JudoDefaultModuleConfiguration`) | `actorResolverLocaleResolutionLevel` |

- **Fix the Guice–Keycloak wiring gap.** `KeycloakLoginInterceptorProvider` now injects an optional
  `PrincipalLocaleConfig` and threads its `localeResolutionLevel` into the interceptor builder, so
  a Guice deployment's ceiling is honoured for the `Accept-Language` capture.

- **Fix the anonymous-branch inconsistency.** `PrincipalLocaleProvider.getLocale()` gates its
  anonymous `RequestLocaleHolder` read behind `level.includes(BROWSER)` — the ceiling now applies
  uniformly to both the authenticated and anonymous browser tiers.

- Update every test using the boolean by the fixed mapping `true → BROWSER`,
  `false → IDENTITY_PROVIDER`, plus new ceiling-scenario tests for `PRINCIPAL`.

### Out of scope

- **Login-time DB write-back.** The `dao.update(...)` call in `DefaultActorResolver.applyLocaleRefresh`
  is already disabled in this branch (Option A of the "backend reads only" review, commented out
  with a re-enable hook). This change only swaps the config type and threads it through; it does
  not touch the persistence code path.
- No frontend template changes; no judo-platform OSGi wiring (deferred with the parent JNG-6415
  work).
- No back-compatibility alias for `browserLanguageCheck` — the feature is unreleased.

## Capabilities

### Modified Capabilities
- `principal-locale-resolution`: replaces the browser-tier boolean gate with the
  `LocaleResolutionLevel` enum ceiling; adds the `IDENTITY_PROVIDER` middle level; fixes two
  latent gaps in how the toggle was propagated (Guice–Keycloak interceptor, anonymous branch).
  Additionally reconciles four normative statements in the parent spec with the implemented
  reality — the previous `Feature gate on principalLocaleAttribute` requirement overreached
  (anonymous requests are unaffected), the `principalLocaleAttribute must be a mapped attribute`
  requirement misdescribed the fallback (no forced default), the pure-helper contract undercounted
  the resolver's public API (three methods, not one), and the `PrincipalLocaleProvider` behaviour
  requirement pointed at `PrincipalVariableProvider.apply(...)` instead of the direct `Context`
  read the implementation actually does.
- `anonymous-request-locale`: reconciles the capture requirement with the implemented reality
  (CXF `AbstractPhaseInterceptor` + `RequestLocaleHolder` thread-local, not a JAX-RS
  `ContainerRequestFilter` writing to `Context`) and locks in the resolved `LOCALE_KEY`
  precedence decision that was left as an Open Question in the original design.

## Impact

- **judo-runtime-core-security**: new `LocaleResolutionLevel` enum (+ test). `PrincipalLocaleResolver`
  signature change (breaking, in-tree only). `PrincipalLocaleConfig` field rename (breaking,
  in-tree only).
- **judo-runtime-core-security-keycloak-cxf**: `KeycloakLoginInterceptor` builder param renamed
  from `Boolean browserLanguageCheck` to `LocaleResolutionLevel localeResolutionLevel`; the private
  field `captureBrowserLanguage` is derived from `level.includes(BROWSER)` and passed unchanged to
  the existing static `captureAcceptLanguage(attributes, request, boolean)` helper (its signature
  is preserved so `KeycloakLoginInterceptorAcceptLanguageTest` compiles unchanged).
- **judo-runtime-core-dispatcher**: `DefaultActorResolver.computeLocaleRefresh(...)` threads
  `config.getLocaleResolutionLevel()` into the resolver call (persistence path unchanged — still
  `dao.update` commented out per the "backend reads only" review). `PrincipalLocaleProvider`'s
  anonymous branch now gates the `Accept-Language` match on `level.includes(BROWSER)`.
- **judo-runtime-core-guice**: `JudoDefaultModuleConfiguration` field rename;
  `JudoDefaultModule` builder param and `PrincipalLocaleConfig` binding switch to the enum.
  `JudoConfigurationQualifiers` — the stale comment referencing the deleted-in-JNG-6415
  `ActorResolverBrowserLanguageCheck` qualifier is refreshed.
- **judo-runtime-core-guice-keycloak**: `KeycloakLoginInterceptorProvider` gains an
  `@Inject(optional=true) PrincipalLocaleConfig` and threads the level into the interceptor
  builder — **fixes the wiring gap** (previously the Guice provider ignored the config entirely).
- **judo-runtime-core-spring**: `JudoDefaultSpringConfiguration.getPrincipalLocaleConfig(...)`
  reads `judo.platform.localeResolutionLevel` (default `BROWSER`) as a `String` and calls
  `LocaleResolutionLevel.parse(...)` — the interceptor is Spring-Boot-starter concern (deferred).
- **Backward compatible**: no — the boolean is removed outright. All in-tree callers migrate in
  the same commit; there are no external callers since JNG-6415 is unreleased.
- **Documentation**: `docs/JNG-6415-multi-language-support.md` env-var table, precedence-table
  footnote, and `KeycloakLoginInterceptor` row updated. Four additional living-doc corrections
  are folded in (see the added tasks): the principal-locale-attribute gate scoping (authenticated
  paths only, anonymous unaffected); the D3a behaviour (no forced default, actor payload
  untouched); the `PrincipalLocaleProviderTest` test count (15 total — 8 original + 7 anonymous);
  the backward-compat wording that overreached ("no signature changes") given the resolver
  builder and provider ctor did in fact grow. The in-flight
  `add-principal-locale-resolution` / `add-anonymous-request-locale` /
  `consolidate-locale-config-object` change docs retain their historical `browserLanguageCheck`
  wording and their as-drafted requirement text (they describe the state at the time they were
  drafted); this new change is the forward-facing spec of record. Reviewers reading the parent
  changes should treat this change's spec deltas as authoritative where they conflict.
