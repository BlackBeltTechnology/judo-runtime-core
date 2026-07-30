## 1. `PrincipalLocaleResolver` (pure helper, TDD)

- [x] 1.1 Create `PrincipalLocaleResolverTest` in `judo-runtime-core-security/src/test/java/hu/blackbelt/judo/runtime/core/security/` with failing tests for: supported-set parsing (comma-separated, trimming, empty), RFC-4647 lookup (`hu` → `hu-HU`; `en-GB` vs `{en-US}` mismatch), tier order (each of browser/claim/stored/default winning), browser gate on/off, unsupported-tag fall-through, malformed-tag skip, blank/null inputs on every tier, terminal `default` always resolves
- [x] 1.2 Run the test class and confirm every test fails (compilation error is acceptable)
- [x] 1.3 Create `PrincipalLocaleResolver` in `judo-runtime-core-security/src/main/java/hu/blackbelt/judo/runtime/core/security/` with the minimal API: `String resolve(String acceptLanguageHeader, String claimLocale, String storedLocale, String defaultLanguage, Set<String> supportedLanguages, boolean browserLanguageCheck)`
- [x] 1.4 Implement supported-set parsing and RFC-4647 lookup as a private helper method
- [x] 1.5 Implement the fixed-order tier walk (browser → claim → stored → default) with the browser gate
- [x] 1.6 Confirm all tests in 1.1 pass; `mvn -pl judo-runtime-core-security test` is green

## 2. `Accept-Language` capture in `KeycloakLoginInterceptor`

- [x] 2.1 Add a failing unit test in `judo-runtime-core-security-keycloak-cxf` covering: gate off ⇒ `__acceptLanguage` absent; gate on + header present ⇒ raw header value in `attributes["__acceptLanguage"]`; gate on + header absent ⇒ key absent
- [x] 2.2 Add a `browserLanguageCheck` boolean field to `KeycloakLoginInterceptor` (or its configuration record) with default `true`
- [x] 2.3 In `handleMessage`, when the gate is `true`, read `Accept-Language` from the `HttpServletRequest` (`message.get("HTTP.REQUEST")`) and put it into the attributes map under the key `__acceptLanguage` before `JudoPrincipal` construction
- [x] 2.4 Confirm the tests in 2.1 pass; `mvn -pl judo-runtime-core-security-keycloak-cxf test` is green

## 3. `DefaultActorResolver` — read-only (no changes)

- [x] 3.1 Read-only decision: the backend does not write the user's stored locale in this change. `DefaultActorResolver` is left untouched by JNG-6415 — no fields, no builder params, no ASM-model introspection, no `DAO.update` call. Persistence of the user's preferred language is deferred to a follow-up capability.
- [x] 3.2 Removed the earlier login-time refresh scaffolding (`refreshActorLocale` / `computeLocaleRefresh` / `applyLocaleRefresh`), the `IdentifierProvider` and `PrincipalLocaleConfig` fields on the resolver, the per-actor-type warned-once set, and the ASM introspection block — all superseded by resolving at read time inside `PrincipalLocaleProvider` (task 6).
- [x] 3.3 Deleted the login-write test classes (`DefaultActorResolverLocaleRefreshTest`, `DefaultActorResolverLocaleRefreshModelTest`). Coverage of the tier walk lives in `PrincipalLocaleProviderTest` and `PrincipalLocaleResolverTest`.
- [x] 3.4 Confirmed `mvn -pl judo-runtime-core-dispatcher test` green after the removal.

## 4. Guice wiring

- [x] 4.1 Bind the `PrincipalLocaleConfig` value object in `judo-runtime-core-guice` from the four app-facing configuration knobs (`actorResolverPrincipalLocaleAttribute`, `actorResolverSupportedLanguages`, `actorResolverDefaultLanguage`, `actorResolverBrowserLanguageCheck`) on `JudoDefaultModuleConfiguration`.
- [x] 4.2 Register `PrincipalLocaleProvider` as the `LocaleProvider` binding (via `PrincipalLocaleProviderProvider`). `DefaultActorResolverProvider` does NOT inject the config — the resolver is locale-agnostic.
- [x] 4.3 Verify `judo-runtime-core-guice` compiles and existing tests pass.

## 5. Spring wiring

- [x] 5.1 Expose a `PrincipalLocaleConfig` `@Bean` in `judo-runtime-core-spring` populated from `@Value("${judo.platform.principalLocaleAttribute:}")` (and the three siblings).
- [x] 5.2 Register `PrincipalLocaleProvider` as a `LocaleProvider` bean consuming that config. The `ActorResolver` bean method does NOT take the config — the resolver is locale-agnostic.
- [x] 5.3 Verify `judo-runtime-core-spring` compiles and existing tests pass.

## 6. `PrincipalLocaleProvider` (backend i18n, read-time tier walk)

- [x] 6.1 Add `hu.blackbelt.osgi.i18n:i18n-api` dependency to `judo-runtime-core-dispatcher`'s pom.
- [x] 6.2 Tests in `PrincipalLocaleProviderTest` cover: authenticated tier walk (browser hint wins over claim wins over stored), unsupported stored value falls through to default, feature gate off, blank / malformed / underscored stored value, principal-with-attribute-only-set, `browserLanguageCheck=false` disables the browser tier, anonymous path (LOCALE_KEY → `RequestLocaleHolder` → default), null context safety, and a regression guard asserting the actor payload is NOT mutated by resolution.
- [x] 6.3 Implement `PrincipalLocaleProvider implements LocaleProvider` in `judo-runtime-core-dispatcher`. On each `getLocale()` call it reads the three candidate inputs from `Context` (browser hint from `JudoPrincipal.attributes[__acceptLanguage]`, claim from `JudoPrincipal.attributes[principalLocaleAttribute]`, stored value from `Context[ACTOR_KEY][principalLocaleAttribute]`) and delegates to `PrincipalLocaleResolver.matchSupportedLanguage(...)` per tier. It does NOT go through `PrincipalVariableProvider.apply(...)` (which triggers a `GET_PRINCIPAL` dispatch and would recurse during error formatting).
- [x] 6.4 Register the provider as `LocaleProvider` in the Guice module (task 4) and as a Spring bean in the autoconfiguration (task 5).
- [x] 6.5 Confirm the tests in 6.2 pass (21/21 green).

## 7. Full build + docs

- [x] 7.1 Full reactor `mvn clean install` green under Java 21, except the Docker-gated PostgreSQL TestContainers tests (environmental — no Docker in this environment; unrelated to this change). Also fixed a null-binding regression in the Guice `browserLanguageCheck` bind surfaced by `JudoDefaultHsqldbModuleTest` (full-injector test).
- [x] 7.2 Added `docs/JNG-6415-multi-language-support.md` (parameters, precedence, feature-off default, mapped-attribute requirement, design notes, config examples, testing).
- [x] 7.3 `openspec validate add-principal-locale-resolution` → valid.

## 8. Deferred (out of scope — follow-up change)

- [ ] 8.1 **Persistence of the user's preferred language.** A separate capability will handle writing the resolved / user-selected locale back to the DB (or Keycloak), including the frontend surface (a "change my language" action) and the read-after-write consistency across sessions. Explicitly out of scope for this change per JNG-6415 review feedback.
- [ ] 8.2 judo-platform OSGi `@AttributeDefinition` mappings for the four `JUDO_PLATFORM_*` env vars and PIDS entries in `DispatcherServiceActivator`.
- [ ] 8.3 OSGi `@Component` service registration for `PrincipalLocaleProvider` so it is picked up by `I18nServiceImpl`'s `@Reference` in OSGi runtimes.
- [ ] 8.4 Frontend template changes (out of scope for this change; tracked separately).
