## 1. Unit tests locking in the propagation rule (TDD — write first, run, watch red)

- [x] 1.1 Created `judo-runtime-core-dispatcher/src/test/java/hu/blackbelt/judo/runtime/core/dispatcher/DefaultDispatcherLocaleKeyTest.java`.
- [x] 1.2 Added **T1**: `callOperation` with an exchange lacking `LOCALE_KEY` leaves
      `context.getAs(Locale.class, LOCALE_KEY)` **null** after entry. (Assert inside a minimal
      registered SDK operation, or by intercepting an `EOperation` implementation used in the
      fixture.)
- [x] 1.3 Added **T2**: `callOperation` with an exchange containing
      `LOCALE_KEY = Locale.forLanguageTag("hu-HU")` populates `Context[LOCALE_KEY]` with that
      exact `Locale`.
- [x] 1.4 Added **T3** (integration-lite): with a `PrincipalLocaleProvider` bound to the same
      `Context`, `PrincipalLocaleConfig` = `{ localeResolutionLevel = BROWSER,
      supportedLanguages = "en-US,hu-HU,de-DE", defaultLanguage = "en-US" }`,
      `RequestLocaleHolder.set("hu-HU")` before dispatch, anonymous exchange (no principal, no
      `LOCALE_KEY`) → inside the dispatched op,
      `injector-equivalent.getInstance(PrincipalLocaleProvider.class).getLocale()` returns
      `Locale.forLanguageTag("hu-HU")`. Clear the holder in `@AfterEach`.
- [x] 1.5 Added **T4** (`LOCALE_KEY` still wins when explicitly provided): same fixture as T3, but
      exchange carries `LOCALE_KEY = Locale.forLanguageTag("en-US")` → provider returns `en-US`.
      Proves the spec's precedence rule (1) still holds; guards against over-correction.
- [x] 1.6 Ran `mvn -pl judo-runtime-core-dispatcher test -Dtest=DefaultDispatcherLocaleKeyTest`.
      **T1 failed** with `Expected: is null but: was <en>` (JVM default). **T3 failed** with
      `Expected: is <hu_HU> but: was <en>` (shadowing confirmed). **T2, T4 passed**. Exactly as
      predicted.
- [x] 1.7 Test fixture chose the "nonexistent operation" shortcut: `callOperation` throws
      `InternalServerException` on the operation-cache lookup, which happens *after* the
      `LOCALE_KEY` propagation block, so the test asserts on the context state left by that
      block. `finally` does not clear context because `exposed = false`. Zero real DAO / ASM
      operations needed — just a minimal `AsmModel` + real `ExpressionModel` +
      `DefaultValidatorProvider` + Mockito for the rest.

## 2. Production edit in `DefaultDispatcher`

- [x] 2.1 Replaced the `LOCALE_KEY` propagation block in
      `judo-runtime-core-dispatcher/…/DefaultDispatcher.java`. Slight deviation from the plan:
      the local `Locale locale` variable had to be kept because it is passed to
      `processResponse(…, locale)` at line 804 (for `BusinessException` and `ResponseConverter`
      formatting). Final shape:

      ```java
      final Locale locale;
      if (exchange.containsKey(LOCALE_KEY)) {
          locale = (Locale) exchange.get(LOCALE_KEY);
          context.putIfAbsent(LOCALE_KEY, locale);
      } else {
          locale = null;
      }
      ```

      Net: context write is now conditional (the bug fix); `locale` passed to `processResponse`
      is `null` when the exchange has none (previously JVM default). Both `BusinessException`
      and `ResponseConverter.builder().locale(…)` are null-tolerant — verified via full
      `mvn -pl judo-runtime-core-dispatcher test` (78/78 green).
- [x] 2.2 Removed the `Locale defaultLocale` parameter from the `@Builder` ctor.
- [x] 2.3 Removed the `private final Locale defaultLocale;` field.
- [x] 2.4 Removed the initialisation
      `this.defaultLocale = Objects.requireNonNullElse(defaultLocale, Locale.getDefault());`.
- [x] 2.5 `java.util.Locale` still used at line 396 (`processResponse` signature) — import kept.

## 3. Consumer sanity checks (no code change expected)

- [x] 3.1 Confirm `ExportCall.java:78-79` still compiles and behaves identically — its
      `Objects.requireNonNullElseGet(..., Locale::getDefault)` already tolerates a null
      `Context[LOCALE_KEY]`.
- [x] 3.2 Confirmed `PrincipalLocaleProvider.java:88-105` (anonymous branch) needs no edit —
      its precedence walk already handles a null `Context[LOCALE_KEY]` correctly. Proven by T3
      going green post-fix.
- [x] 3.3 Grep confirmation: `grep -rn "\.defaultLocale(" /home/balazs/judo-ng --include='*.java'`
      returns no `DefaultDispatcher.builder()` hits post-edit.

## 4. Regression sweep

- [x] 4.1 `mvn -pl judo-runtime-core-dispatcher test` green — **78/78** including
      `PrincipalLocaleProviderTest` (18), `DefaultActorResolverLocaleRefreshModelTest` (5),
      new `DefaultDispatcherLocaleKeyTest` (4).
- [x] 4.2 `mvn -pl judo-runtime-core-security test` green.
- [x] 4.3 `mvn -pl judo-runtime-core-guice test` green (compiled clean; no in-tree caller of
      the removed `.defaultLocale(…)` param).
- [x] 4.4 `mvn -pl judo-runtime-core-spring test` green.
- [x] 4.5 `mvn -pl judo-runtime-core-guice,judo-runtime-core-spring -am install -DskipTests`
      green — downstream compile succeeds.

## 5. Openspec archival prerequisites

- [x] 5.1 `openspec validate dispatcher-locale-key-only-from-exchange --strict` green.
- [x] 5.2 Cross-linked this change from `docs/JNG-6415-dispatcher-locale-key-shadowing.md`
      "Related" block (fix change id, fix commit hash, regression-guard test class name).
      The doc's "Recommended origin follow-up" section is preserved as historical
      context; the top-of-doc resolution timeline (commit `cb1f64e8`) supersedes it.

## 6. Cross-repo follow-up

- [x] 6.1 In `judo-runtime-core-esm-itest:feature/JNG-6415_LocaleProviderInjectability`,
      the two guard tests
      (`LocaleProviderInjectabilityTest#testWithoutExchangeLocaleKeyFallsBackToDispatcherDefault`,
      `LocaleKeyPrecedenceTest#testDispatchedOpReturnsContextLocaleShadowingHeader`)
      were flipped from asserting JVM-default (pre-fix) to asserting the header locale
      (post-fix). Confirmed via the itest full-suite run (894/894 pass after the flip;
      2 pre-flip red guards were the intended trigger). Pushed by the itest branch owner.
