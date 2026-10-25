## Context

Facts verified against the current tree (post-revert of the "backend reads only" commit; on the
Option A footing where the login refresh machinery is preserved with `dao.update` commented out):

- **The boolean has three concrete propagation sites**, all traversed today:
  1. `PrincipalLocaleResolver.resolve(..., boolean browserLanguageCheck)` — the pure tier walker.
  2. `PrincipalLocaleConfig.browserLanguageCheck` (Boolean, `@Builder.Default = TRUE`) — the value
     object that carries it through Guice/Spring wiring.
  3. `KeycloakLoginInterceptor` — the CXF phase interceptor that decides whether to read
     `Accept-Language` off the `HttpServletRequest` and stash it into principal attributes.
- **Guice–Keycloak gap.** `KeycloakLoginInterceptorProvider`
  (`judo-runtime-core-guice-keycloak/src/main/java/…/providers/KeycloakLoginInterceptorProvider.java`)
  has fields for `models`, `openIdConfigurationProvider`, `realmExtractor`,
  `transformationTraceService`, `acceptableClients` — but **no** `PrincipalLocaleConfig` /
  `browserLanguageCheck` field. The interceptor is built with the `@Builder.Default = TRUE`. Under
  Guice, setting `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK=false` today has no effect on capture.
- **Anonymous branch inconsistency.** `PrincipalLocaleProvider.getLocale()` in
  `judo-runtime-core-dispatcher` gates the browser tier of the authenticated walk behind the
  boolean, but the anonymous branch (no principal bound) matches `RequestLocaleHolder` against
  supported languages **unconditionally** — the boolean-off intent is not honoured there.
- **Test surface.** `PrincipalLocaleResolverTest` calls `resolve(..., true|false)` throughout;
  `PrincipalLocaleConfigTest` asserts the boolean field; `DefaultActorResolverLocaleRefreshTest`
  and `DefaultActorResolverLocaleRefreshModelTest` build configs with `.browserLanguageCheck(true)`;
  `KeycloakLoginInterceptorAcceptLanguageTest` calls the **static helper**
  `captureAcceptLanguage(..., boolean)` directly, so its call sites are boolean-native (i.e., the
  static helper's signature must not change).

## Goals / Non-Goals

**Goals:**
- Express the three legitimate deployment postures — full browser trust, IdP-only, principal-only
  — as a single, self-describing enum.
- Ensure the ceiling propagates uniformly: authenticated tier walk (already gated), anonymous
  branch (currently ungated — fix), Keycloak capture under both Guice and Spring (currently only
  Spring — fix).
- Preserve every existing behaviour under the `true → BROWSER`, `false → IDENTITY_PROVIDER`
  mapping so no test needs a semantic revisit beyond the mechanical rename.
- Keep the change surface **small and localised**: one new enum, three type substitutions
  (resolver signature, config field, interceptor builder), two wiring renames (Guice+Spring), two
  bug fixes (Guice–Keycloak, anonymous branch). No downstream API breaks (JNG-6415 is unreleased).

**Non-Goals:**
- No back-compatibility alias for the removed boolean (no `browserLanguageCheck` setter, no
  `judo.platform.browserLanguageCheck` deprecation). The feature is unreleased.
- No new resolution tier (still browser / claim / stored / default). No `languageOrder` knob.
- No change to the persistence path (`dao.update` stays commented out per the parent branch's
  Option A footing; enabling it is a separate future capability).
- No frontend, no OSGi platform wiring.

## Decisions

### D1. Enum ceiling, not toggle set

`LocaleResolutionLevel` names the **highest-priority tier the runtime is allowed to consult**.
Lower tiers are always active — the enum is a ceiling, not a bitmask. This preserves the "lower
tier always fires when higher tiers are absent" fall-through semantics of the current resolver
without introducing per-tier flags.

**Alternative considered (rejected):** three independent booleans (`browserEnabled`,
`claimEnabled`, `storedEnabled`). Rejected because six of the eight combinations are either
meaningless ("stored off but browser on"), unsafe (all off), or duplicate an existing state — the
underlying deployment story really is "how much of the request do I trust?", which is a single
axis, not three.

### D2. Ascending ordinal so `ordinal() >= tier.ordinal()` = "included"

Constants are declared `PRINCIPAL, IDENTITY_PROVIDER, BROWSER` — the enum value at ordinal `N`
means "the runtime is allowed to look at tiers up to and including ordinal `N`". The `includes`
check is a one-liner (`return ordinal() >= tier.ordinal();`) that reads left-to-right the way the
concept does: `BROWSER.includes(IDENTITY_PROVIDER)` → true, `PRINCIPAL.includes(BROWSER)` → false.

**Alternative considered (rejected):** descending ordinal (`BROWSER=0, IDENTITY_PROVIDER=1,
PRINCIPAL=2`) so the ordinal matches "how restrictive" the level is. Rejected because
`Enum.valueOf` / persistence layers order by ordinal, and reviewers reading the enum in isolation
find "increasing ordinal = increasing capability" more intuitive.

### D3. Lenient `parse(String)`, never throws

Environment variables and Spring property values arrive as arbitrary strings. `parse(null | blank
| garbage) → BROWSER` (the default) so a typo in the env var name reduces to the safe default
instead of blowing up authentication. Trimming and `Locale.ROOT`-lowered comparison tolerate the
usual UPPER-vs-lower / stray-whitespace variance. Rationale mirrors
`AcceptableClientsParser.parse` and `PrincipalLocaleResolver.parseSupportedLanguages` — every
platform-parameter parser in this module is lenient by construction.

### D4. Fixed `true → BROWSER` / `false → IDENTITY_PROVIDER` mapping for legacy test rewrites

`true` (default) enabled the browser tier, so it maps to `BROWSER`. `false` disabled the browser
tier but still allowed claim + stored, so it maps to `IDENTITY_PROVIDER` (not `PRINCIPAL`). This
preserves every existing test's observable outcome; no new test needs to change its assertions
because of the enum swap alone. `PRINCIPAL` is a **new** capability that has no historical
counterpart, so it gets new dedicated scenario tests.

### D5. No back-compat alias — replace outright

The parent `JNG-6415` branch has not shipped. Adding a `Deprecated` alias for
`browserLanguageCheck` would carry a boolean-shaped API into perpetuity for a knob no external
consumer has ever seen. Ripping it out now keeps the enum's semantics clean.

### D6. Keycloak interceptor builder takes the enum; the static helper still takes a boolean

`KeycloakLoginInterceptor` exposes a `@Builder` param `LocaleResolutionLevel localeResolutionLevel`
and derives an internal `boolean captureBrowserLanguage = level == null || level.includes(BROWSER)`.
That boolean is passed to the existing package-static helper `captureAcceptLanguage(attributes,
request, boolean captureBrowserLanguage)` — the helper's signature is intentionally preserved so
`KeycloakLoginInterceptorAcceptLanguageTest` (which unit-tests the helper directly with true/false)
compiles and passes unchanged.

**Rationale:** the enum belongs at the *configuration* boundary. Deep inside the interceptor the
question is still "capture or not?", which is a boolean. Keeping the helper boolean-native avoids
churn in six tests that were themselves recent adds under the parent JNG-6415 change.

### D7. Fix the Guice–Keycloak wiring gap in the same change

`KeycloakLoginInterceptorProvider` gains an `@Inject(optional = true) @Nullable
PrincipalLocaleConfig localeConfig` and passes
`localeConfig != null ? localeConfig.getLocaleResolutionLevel() : null` to the interceptor
builder. The null-tolerance matches the existing pattern for `acceptableClients` in the same
class. This is technically a bug fix independent of the enum rename, but they are inseparable in
practice — the same wiring change carries both, and the enum's `PRINCIPAL` level would be
untestable under Guice without it.

### D8. Fix the anonymous-branch inconsistency in the same change

`PrincipalLocaleProvider.getLocale()`'s anonymous branch now guards the `RequestLocaleHolder` read
behind `localeConfig.getLocaleResolutionLevel().includes(BROWSER)`. Without this the enum ceiling
would be a half-truth ("browser gated for authenticated requests, unconditional for anonymous
requests"). This is a small semantic change already implied by the enum's contract, and it
matches D7's pattern of "fix latent inconsistencies while we're already in the file".

### D9. Reconcile four normative statements in the parent `principal-locale-resolution` spec

A post-implementation review of the parent `add-principal-locale-resolution` spec surfaced four
requirement clauses that no longer match the implemented behaviour. Because JNG-6415 is
unreleased and this change is already the forward-facing spec of record, we express the
corrections as MODIFIED requirements here rather than rewriting the historical change docs
(which document the state at the time they were drafted).

- **Feature gate overreach.** The parent's `Feature gate on principalLocaleAttribute` requirement
  asserted `PrincipalLocaleProvider.getLocale()` "SHALL return the default (identical to unbound
  LocaleProvider)" whenever `principalLocaleAttribute` is unset. Verified against the code: the
  gate only disables the **authenticated** paths (Keycloak `Accept-Language` capture, the
  authenticated tier walk in `resolvePrincipalLocaleTag()`); the anonymous branch (introduced by
  `add-anonymous-request-locale`) has its own gate (`supportedLanguages` non-empty) and continues
  to resolve for anonymous requests regardless of `principalLocaleAttribute`.
- **D3a fallback misdescription.** The parent's `principalLocaleAttribute must be a mapped
  attribute` requirement said the request "SHALL proceed with the resolved locale determined via
  the fallback chain". Verified against `DefaultActorResolver.computeLocaleRefresh`: when the
  attribute is non-mapped or `transient`, the method returns `null` and neither `DAO.update` nor
  the in-request `result.put(...)` runs. The actor payload's stored value is left as-is (whatever
  the DAO returned, which may be `null`); no default is forced. `PrincipalLocaleProvider` computes
  the effective locale from the untouched payload via the usual tier walk.
- **Resolver API undercount.** The parent's `PrincipalLocaleResolver is a pure helper` requirement
  said the public API is "a single method". Verified against the source: three public statics
  ship — `resolve(...)` (full tier walker), `matchSupportedLanguage(candidate, supported)`
  (single-tier filter used by the anonymous branch), and `parseSupportedLanguages(csv)`
  (memoised once by `PrincipalLocaleConfig`).
- **Provider read path.** The parent's `Backend messages follow the resolved locale via
  PrincipalLocaleProvider` requirement said the provider reads via
  `PrincipalVariableProvider.apply(principalLocaleAttribute)`. Verified against the impl: it
  reads `Context` directly (`ACTOR_KEY` payload, then `PRINCIPAL_KEY` attributes). The deviation
  is intentional and already captured in `add-principal-locale-resolution/tasks.md` §6.3 as a
  NOTE; the requirement itself never caught up.

See the MODIFIED entries in `specs/principal-locale-resolution/spec.md` for the normative text.

### D10. Reconcile the `anonymous-request-locale` capture path with the implementation

The parent `add-anonymous-request-locale` proposal/design described a JAX-RS
`ContainerRequestFilter` in `judo-runtime-core-jaxrs-cxf-server` that stashes the header into
`Context` under a dedicated key. The implementation shipped a CXF `AbstractPhaseInterceptor`
(RECEIVE phase) in `judo-runtime-core-jaxrs-cxf` (`AcceptLanguageCaptureInterceptor`) that sets
a thread-local (`RequestLocaleHolder` in the `judo-runtime-core` module). The rationale for the
choice is already recorded in the parent design (`DefaultDispatcher.callOperation` calls
`context.removeAll()` for exposed HTTP-entry operations, wiping anything a JAX-RS filter placed
in `Context`; a thread-local set by a transport-phase interceptor survives that reset).

Express the corrected requirement here. Also lock in the `LOCALE_KEY` precedence decision that
was left open in the parent design's Open Questions ("Should the provider also honor an app-set
`LOCALE_KEY`... Leaning yes, to be finalized in the spec."): **yes** — app-set `LOCALE_KEY`
takes precedence over the raw captured header. The implementation already does this
(`PrincipalLocaleProvider.getLocale()` in the anonymous branch reads `LOCALE_KEY` before
`RequestLocaleHolder`); the test `anonymousApplicationSetLocaleKeyWins` locks it in. This change
makes the decision normative.

See the new `specs/anonymous-request-locale/spec.md` in this change for the normative text.

### D11. Test-count wording in the living reference doc

`docs/JNG-6415-multi-language-support.md` §9 (Testing) currently reads
"`PrincipalLocaleProviderTest` (dispatcher) — 8 tests". After `add-anonymous-request-locale`
extended the class with 7 anonymous-branch scenarios, the class holds 15 `@Test` methods
(verified via `grep -c '@Test'`). Update the living doc to "15 tests total (8 original + 7
anonymous-branch cases from `add-anonymous-request-locale`)" so contributors reading the doc get
a true count and the historical context.

### D12. "No signature changes" wording is inaccurate; correct in the living doc only

`add-principal-locale-resolution/proposal.md` claims "No interface or signature changes" as part
of its Backward Compatibility note. Verified against the source: `DefaultActorResolver.builder()`
grew five new optional fields, and the new `PrincipalLocaleProvider` class has a ctor whose
typing evolved across the JNG-6415 iteration (JNG-6415 added parameters; `consolidate` collapsed
them into `PrincipalLocaleConfig`). The public JUDO interfaces (`DAO`, `Dispatcher`,
`LocaleProvider`) themselves are unchanged. We do **not** rewrite the historical proposal; we
note the correction in the living doc for anyone auditing the branch's compatibility promises.

## Risks / Trade-offs

- **Silent behaviour change for deployments that set `JUDO_PLATFORM_BROWSER_LANGUAGE_CHECK=false`
  under Guice.** Today the toggle is ignored (D-context bullet). After this change it is honoured.
  Deployments that relied on the bug (i.e., that set the toggle off but observed the header still
  captured) will see the header stop being captured. Mitigated by the branch being unreleased —
  no such deployment exists.
- **Anonymous branch behaviour change** (analogous). Deployments that set the toggle off expected
  anonymous requests to also ignore the header; today they don't. After this change they do.
  Mitigated the same way.
- **Naming churn radius.** Every `browserLanguageCheck` reference across code, tests, docs, and
  one env var must move together. The proposal lists all sites; the change is entirely mechanical
  after the enum lands.
- **Documentation drift.** Three in-flight OpenSpec changes (`add-principal-locale-resolution`,
  `add-anonymous-request-locale`, `consolidate-locale-config-object`) still describe the boolean.
  We intentionally do NOT rewrite them — they document the state at the time they were proposed;
  this change is the forward-facing spec of record.

## Migration Plan

- **Deploy:** ship the enum in the same commit as the rename of the env var / Spring prop /
  Guice config field. Deployments configuring the toggle apply the fixed mapping in the same
  release. Because the parent branch is unreleased, no user-facing communication is required.
- **Rollback:** revert the commit. `browserLanguageCheck` returns; the wiring gap and anonymous
  inconsistency return with it (they are pre-existing).
- **Backfill:** none.

## Open Questions

- **Task 0 (integration test scope).** The feedback asks for a testkit test proving
  `LocaleProvider` is injectable into a backend `Function<Payload,Payload>` operation and that its
  `getLocale()` return changes with `LocaleResolutionLevel`. Whether that lives in
  `judo-runtime-core-guice-hsqldb` (piggybacks on `JudoDefaultHsqldbModuleTest`'s full-injector
  wiring — cheaper) or `judo-runtime-core-guice-testkit` (matches the feedback's literal suggestion
  — closer to how apps consume the runtime) is deferred to the implementing PR and asked in
  `tasks.md` §0.
