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

## 3. `DefaultActorResolver` refresh

- [x] 3.1 Add failing tests to `judo-runtime-core-dispatcher` covering: feature gate off (blank `principalLocaleAttribute`) ⇒ no `PrincipalLocaleResolver` call, no `dao.update`; resolved != stored ⇒ `dao.update` called with `{id, localeAttr}`; resolved == stored ⇒ no update; `dao.update` throws ⇒ WARN + auth continues; `principalLocaleAttribute` names a non-mapped or transient attribute ⇒ no update, one-time WARN
- [x] 3.2 Add four fields to `DefaultActorResolver` (`principalLocaleAttribute`, `supportedLanguages`, `defaultLanguage`, `browserLanguageCheck`), wired through the `@Builder` constructor
- [x] 3.3 In `getActorByClaims`, after `dao.search(...)` result is loaded, when `principalLocaleAttribute` is non-blank: read stored value from payload, read claim locale, read `__acceptLanguage` from claims, invoke `PrincipalLocaleResolver`, and if the resolved value differs from stored, call `dao.update(actorType, payloadWithIdAndLocale, null)` inside `try/catch(WARN)`
- [x] 3.4 Implement the mapped-attribute check (D3a): before the first refresh per actor type, verify the attribute exists on the `EClass` and is not transient; on failure, WARN once and set a per-type flag to skip subsequent refresh attempts
- [x] 3.5 Confirm tests in 3.1 pass; `mvn -pl judo-runtime-core-dispatcher test` is green
- [x] 3.6 Added `DefaultActorResolverLocaleRefreshModelTest` — integration test against a **real ASM model** (built with `EcoreBuilders` + `AsmModelResourceSupport`) covering the D3a introspection glue (`isMappedTransferObjectType`, `getMappedAttribute`, `transient` check) with a mocked DAO. 5 scenarios: claim≠stored persists, browser tier wins, resolved==stored no-op, transient attribute not persisted, model-introspection sanity.

## 4. Guice wiring

- [x] 4.1 Add bindings in `judo-runtime-core-guice` for the four `@Named` parameters used by `DefaultActorResolver`
- [x] 4.2 Follow the existing `checkMappedActors` / `acceptableClients` pattern for optional binding with sensible defaults
- [x] 4.3 Verify `judo-runtime-core-guice` compiles and existing tests pass

## 5. Spring wiring

- [x] 5.1 Add `@Value("${judo.platform.principalLocaleAttribute:}")` and matching bindings for the other three parameters in the Spring autoconfiguration (`judo-runtime-core-spring`)
- [x] 5.2 Ensure defaults match the spec (`supportedLanguages` defaults to `{defaultLanguage}`; `defaultLanguage` defaults to JVM default or `en-US`; `browserLanguageCheck` defaults to `true`)
- [x] 5.3 Verify `judo-runtime-core-spring` compiles and existing tests pass

## 6. `PrincipalLocaleProvider` (backend i18n)

- [x] 6.1 Add `hu.blackbelt.osgi.i18n:i18n-api` dependency to `judo-runtime-core-dispatcher`'s pom
- [x] 6.2 Add failing tests for `PrincipalLocaleProvider`: principal-with-locale ⇒ `Locale.forLanguageTag("hu-HU")`; no principal ⇒ default; malformed stored value ⇒ default without throw
- [x] 6.3 Implement `PrincipalLocaleProvider implements LocaleProvider` in `judo-runtime-core-dispatcher`. NOTE: deviated from `PrincipalVariableProvider.apply(...)` (which triggers a full GET_PRINCIPAL dispatch on every message-key lookup) in favour of a cheap O(1) read from the request-scoped `Context` (ACTOR_KEY then PRINCIPAL_KEY), falling back to `defaultLanguage`
- [x] 6.4 Register the provider as `LocaleProvider` in the Guice module (task 4) and as a Spring bean in the autoconfiguration (task 5)
- [x] 6.5 Confirm the tests in 6.2 pass

## 7. Full build + docs

- [x] 7.1 Full reactor `mvn clean install` green under Java 21, except the Docker-gated PostgreSQL TestContainers tests (environmental — no Docker in this environment; unrelated to this change). Also fixed a null-binding regression in the Guice `browserLanguageCheck` bind surfaced by `JudoDefaultHsqldbModuleTest` (full-injector test).
- [x] 7.2 Added `docs/JNG-6415-multi-language-support.md` (parameters, precedence, feature-off default, mapped-attribute requirement, design notes, config examples, testing).
- [x] 7.3 `openspec validate add-principal-locale-resolution` → valid.

## 8. Deferred (out of scope — follow-up change)

- [ ] 8.1 judo-platform OSGi `@AttributeDefinition` mappings for the four `JUDO_PLATFORM_*` env vars and PIDS entries in `DispatcherServiceActivator` / `DefaultActorResolverComponent`
- [ ] 8.2 OSGi `@Component` service registration for `PrincipalLocaleProvider` so it is picked up by `I18nServiceImpl`'s `@Reference` in OSGi runtimes
- [ ] 8.3 Frontend template changes (out of scope for this change; tracked separately)
