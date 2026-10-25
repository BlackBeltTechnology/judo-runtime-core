# JNG-6415 Task 0 — LocaleProvider injectability into a backend operation
### Handoff to a `judo-runtime-core-esm-itest` agent

> **Origin change:** `openspec/changes/replace-browser-check-with-resolution-level/`
> in `judo-runtime-core` (branch `feature/JNG-6415_MultiLanguageSupport`).
> **Origin task:** §0.2 / §0.3 of that change's `tasks.md` — 39/46 done in the origin branch;
> this handoff covers the three remaining implementation tasks (0.2, 0.3, and the local half
> of 0.4).
> **Host repo:** `judo-runtime-core-esm-itest` (git submodule at
> `/home/balazs/judo-ng/runtime/judo-runtime-core-esm-itest`, separate from the
> `judo-runtime-core` reactor).
> **Suggested branch name:** `feature/JNG-6415_LocaleProviderInjectability`.

---

## 1. Why this exists

The `replace-browser-check-with-resolution-level` change in `judo-runtime-core` adds a
`LocaleResolutionLevel` enum (`PRINCIPAL < IDENTITY_PROVIDER < BROWSER`, default `BROWSER`) that
governs which tiers the runtime consults when resolving a user's effective locale for backend
message i18n. The enum is wired through `PrincipalLocaleConfig` → `PrincipalLocaleProvider` →
`I18nServiceImpl` under Guice, and separately under Spring.

The change spec asserts (`specs/principal-locale-resolution/spec.md`, ADDED requirement
**"Backend operations may inject `LocaleProvider`"**) that a custom backend operation supplied as a
`Function<Payload,Payload>` can `@Inject LocaleProvider`, and that the returned `Optional<Locale>`
reflects the configured `LocaleResolutionLevel` for the current request. This integration test
proves that promise end-to-end.

The test cannot live in `judo-runtime-core` proper because none of its modules assemble a
runnable ESM-model-backed app with `Function<Payload,Payload>` operations. `judo-runtime-core-esm-itest`
already does exactly that.

---

## 2. Prerequisites

- **Installed SNAPSHOT.** `judo-runtime-core-esm-itest` depends on released `judo-runtime-core`
  artifacts. The enum lives in `hu.blackbelt.judo.runtime:judo-runtime-core-security:1.0.6-SNAPSHOT`.
  Before running, install the origin branch:
  ```bash
  cd /home/balazs/judo-ng/runtime/judo-runtime-core
  mvn install -DskipTests -pl '!judo-runtime-core-guice-postgresql'
  ```
  Confirm the SNAPSHOT is resolvable:
  ```bash
  ls ~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-security/1.0.6-SNAPSHOT/
  ```
- **Version bump in this repo.** If `judo-runtime-core-esm-itest`'s pom pins a released
  `judo-runtime-core` version, bump it to `1.0.6-SNAPSHOT` in the feature branch. Grep for the
  runtime-core version property and update:
  ```bash
  cd /home/balazs/judo-ng/runtime/judo-runtime-core-esm-itest
  grep -n "judo-runtime-core.*version\|<judo-runtime-core" pom.xml
  ```
- **JDK 21** (same as origin).

---

## 3. What to build

A new test class in `src/test/java/hu/blackbelt/judo/runtime/core/dao/rdbms/locale/LocaleProviderInjectabilityTest.java`:

- Uses the standard `@ExtendWith(JudoDatasourceSingletonExtension.class)` +
  `@ExtendWith(JudoRuntimeExtension.class)` fixture pattern (see
  `src/test/java/hu/blackbelt/judo/runtime/core/dao/rdbms/script/EscapedCharactersTest.java` for
  the canonical shape).
- Builds a minimal ESM model via `PsmTestModelBuilder`:
  - One entity, one **unbound operation** `whichLocale` (input `None`, output a transient TO with a
    single `String` field `tag`).
- Registers a custom `Function<Payload,Payload>` for `whichLocale` via the fixture's
  `extraModule` Guice override — the impl `@Inject`s `LocaleProvider` (or grabs it from the
  injector) and returns `Payload.map("tag", provider.getLocale().map(Locale::toLanguageTag).orElse(""))`.
- Wires `PrincipalLocaleConfig` in the same `extraModule` — a fresh instance per test case with
  a specific `LocaleResolutionLevel`.
- Asserts three scenarios that flip the returned tag by ceiling.

### 3.1 Fixture seam

`JudoRuntimeFixture` (in
`src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/fixture/JudoRuntimeFixture.java`) already
exposes:
- `@Setter Module extraModule;` — Guice module that overrides `JudoDefaultModule` bindings
  (see line ~360; `Modules.override(judoDefaultModule).with(extraModule)`).
- `getOperationImplementations()` returning `Map<String, Function<Payload,Payload>>` — the map the
  test's `.apply(exchange)` reads from.

Bind the custom operation impl via the `SdkFunctions` map in `DispatcherFunctionProvider` (mirror
what the fixture's script path does at line ~454 with `getScriptFunctions().putAll(scripts)`). The
alternative — subclassing `DispatcherFunctionProvider` in the extra module — is fine too; pick
whichever is smaller for one operation.

### 3.2 Locale-config override in the extra module

```java
public class LocaleTestModule extends AbstractModule {
    private final LocaleResolutionLevel level;
    private final String acceptLanguageHint; // may be null
    LocaleTestModule(LocaleResolutionLevel level, String acceptLanguageHint) { ... }

    @Override protected void configure() {
        bind(PrincipalLocaleConfig.class).toInstance(PrincipalLocaleConfig.builder()
                .principalLocaleAttribute("locale")            // authenticated path off unless set
                .supportedLanguages("en-US,hu-HU,de-DE")
                .defaultLanguage("en-US")
                .localeResolutionLevel(level)
                .build());
        // If simulating a browser hint: RequestLocaleHolder.set(acceptLanguageHint) in an @BeforeEach.
    }
}
```

`RequestLocaleHolder` lives in `hu.blackbelt.judo.runtime.core` (`judo-runtime-core` module).
Since these tests do not have a real HTTP interceptor, set/clear the thread-local directly around
each scenario.

### 3.3 Test scenarios (minimum)

Three `@Test` methods; each builds a fresh fixture with a different `LocaleResolutionLevel` in the
extra module, calls the `whichLocale` operation via the fixture, and asserts the tag.

| # | `LocaleResolutionLevel` | `RequestLocaleHolder` | Expected `tag` |
|---|---|---|---|
| 1 | `BROWSER` | `hu-HU` | `hu-HU` |
| 2 | `IDENTITY_PROVIDER` | `hu-HU` (ignored — above ceiling) | `en-US` (default) |
| 3 | `PRINCIPAL` | `hu-HU` (ignored) | `en-US` (default) |

**These three cases are sufficient to prove the ceiling propagates through the injected
provider.** They correspond to the `Injected provider reflects the resolution level` and
`Ceiling change flips the return` scenarios in the change's ADDED requirement.

### 3.4 Task 0.3 assertion — no DB write-back

Add one more `@Test` that runs the `BROWSER` scenario twice with the fixture's DB re-loaded in
between, and asserts the DB row's `locale` column is **unchanged** across the two invocations.
This locks in the "backend reads only" decision from Option A on the origin branch — see
`docs/JNG-6415-multi-language-support.md` §7 in the origin repo. The `dao.update` in
`DefaultActorResolver.applyLocaleRefresh` is currently commented out; re-enabling it (a future
capability) would silently flip this test to fail, which is exactly the regression guard we want.

If exercising authenticated actor persistence is too heavy for one integration test, an
alternative — noted in the origin change's task 0.3 — is: assert the resolved locale returned by
the operation is correct while `dao.update` is disabled, and add a comment referencing the
"backend reads only" review decision as the reason no persistence assertion accompanies it. Pick
whichever is cheaper; document the choice in the test's javadoc.

---

## 4. Verification

- `mvn test -Dtest=LocaleProviderInjectabilityTest` — green with the SNAPSHOT installed.
- All existing tests in `judo-runtime-core-esm-itest` remain green (`mvn test` at the repo root).
- The new test lives beside sibling `dao/rdbms/*` tests and follows their fixture idioms so it is
  discoverable by anyone reading the module.

---

## 5. What to update back in the origin branch

Once this handoff lands (or the branch is at least open for review), the origin branch's tasks
file should be updated:

- `openspec/changes/replace-browser-check-with-resolution-level/tasks.md` §0.2 currently says the
  host is "`judo-runtime-core-guice-hsqldb` (extends `JudoDefaultHsqldbModuleTest`) ... unless the
  reviewer prefers `judo-runtime-core-guice-testkit`". Replace with a note that Task 0
  implementation lives in the sibling `judo-runtime-core-esm-itest` repo on branch
  `feature/JNG-6415_LocaleProviderInjectability`, with a link to that branch's PR when open.
- Once merged in the itest repo, tick §0.2 and §0.3 in the origin tasks.md and commit.

§0.4 (app-level demonstrator operation in a generated app) stays deferred — it's an out-of-tree
demonstration for the frontend team, not something this itest covers.

---

## 6. Pointers into the origin branch (for context while writing the test)

- **Enum:** `judo-runtime-core-security/src/main/java/hu/blackbelt/judo/runtime/core/security/LocaleResolutionLevel.java`.
- **Config value object:** `judo-runtime-core-security/src/main/java/hu/blackbelt/judo/runtime/core/security/PrincipalLocaleConfig.java`
  (Lombok `@Value @Builder`; `@Builder.Default localeResolutionLevel = BROWSER`; memoised
  `getParsedSupportedLanguages()`; `isEnabled()`).
- **Provider (authenticated + anonymous):**
  `judo-runtime-core-dispatcher/src/main/java/hu/blackbelt/judo/runtime/core/dispatcher/environment/PrincipalLocaleProvider.java`.
  Reads `Context[ACTOR_KEY]` → `Context[PRINCIPAL_KEY]` → for anonymous:
  `Context[LOCALE_KEY]` → `RequestLocaleHolder.getAcceptLanguage()` filtered against
  `supportedLanguages` (gated by ceiling) → `defaultLanguage`.
- **Thread-local holder:** `judo-runtime-core/src/main/java/hu/blackbelt/judo/runtime/core/RequestLocaleHolder.java`.
- **Guice binding:** `judo-runtime-core-guice/src/main/java/hu/blackbelt/judo/runtime/core/guice/JudoDefaultModule.java`
  §`configureLocaleProvider()` and §`configureOptions()` (`PrincipalLocaleConfig` bind).
- **Spec:** `openspec/changes/replace-browser-check-with-resolution-level/specs/principal-locale-resolution/spec.md`,
  ADDED requirement "Backend operations may inject `LocaleProvider`" — the two normative
  scenarios that this integration test proves.
- **Design D6, D7, D8:** same change's `design.md` — explains why the provider reads `Context`
  directly and why the Guice–Keycloak wiring and anonymous branch were fixed alongside the enum
  swap.

---

## 7. Minimal effort estimate

- 1 test class (~200 lines including model builder + assertions).
- 1 extra Guice module class (~30 lines) or an inline anonymous module in the test.
- No production code changes in `judo-runtime-core-esm-itest`.
- ~1 hour of implementation + local verification.

If the fixture surface makes registering an SDK operation impl awkward (i.e., `SdkFunctions` map
isn't cleanly wired for external overrides at test time), the fallback is to add a small helper
method to `JudoRuntimeFixture` that accepts a `Map<String, Function<Payload,Payload>>` and puts
it into `getDispatcherFunctionProvider().getSdkFunctions()`. Keep any such addition minimal and
purely additive — the fixture is depended upon by other itest modules.
