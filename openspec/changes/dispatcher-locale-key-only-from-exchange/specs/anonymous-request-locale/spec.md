## ADDED Requirements

### Requirement: Anonymous browser tier is reachable through `DefaultDispatcher.callOperation`

The anonymous-branch precedence walk defined by the parent `anonymous-request-locale` capability SHALL hold end-to-end through a dispatched operation, not only in `PrincipalLocaleProvider` unit-test isolation. The walk order — `Context[LOCALE_KEY]` → `RequestLocaleHolder.getAcceptLanguage()` (subject to the `LocaleResolutionLevel` ceiling) → `defaultLanguage` — is unchanged from the parent capability.

Specifically: when `DefaultDispatcher.callOperation` is invoked with an anonymous exchange that
carries no `LOCALE_KEY`, `PrincipalLocaleProvider.getLocale()` invoked from inside the dispatched
operation body SHALL consult `RequestLocaleHolder.getAcceptLanguage()` (rule 2), not a
dispatcher-synthesised `Context[LOCALE_KEY]` (rule 1).

> Change note: this requirement locks in the post-fix behaviour delivered by the sibling
> `dispatcher` capability's "`DefaultDispatcher.callOperation` propagates `LOCALE_KEY` only from
> the exchange" requirement. Before that fix, the pre-existing precedence scenarios in the
> parent `anonymous-request-locale` spec ("Header used when `LOCALE_KEY` is absent", "Default
> fallback when both are absent") were unreachable through a dispatched op because
> `callOperation` unconditionally populated `Context[LOCALE_KEY]` with `Locale.getDefault()`.
> This scenario proves they are now reachable. Reproducer:
> `docs/JNG-6415-dispatcher-locale-key-shadowing.md`.

#### Scenario: BROWSER ceiling, empty exchange, header wins through dispatch
- **GIVEN** `PrincipalLocaleConfig` = `{ localeResolutionLevel = BROWSER,
  supportedLanguages = "en-US,hu-HU,de-DE", defaultLanguage = "en-US" }`
- **AND** no principal is bound
- **AND** `RequestLocaleHolder.set("hu-HU")` has been invoked (as by
  `AcceptLanguageCaptureInterceptor`)
- **WHEN** `DefaultDispatcher.callOperation` is invoked with an exchange containing no
  `LOCALE_KEY`
- **AND** the dispatched operation body invokes `PrincipalLocaleProvider.getLocale()`
- **THEN** the returned locale SHALL be `Optional.of(Locale.forLanguageTag("hu-HU"))`
- **AND** the returned locale SHALL NOT be `Locale.getDefault()`

#### Scenario: BROWSER ceiling, exchange-provided `LOCALE_KEY` still wins
- **GIVEN** the same `PrincipalLocaleConfig` and holder state as the preceding scenario
- **WHEN** `DefaultDispatcher.callOperation` is invoked with an exchange containing
  `LOCALE_KEY = Locale.forLanguageTag("en-US")`
- **AND** the dispatched operation body invokes `PrincipalLocaleProvider.getLocale()`
- **THEN** the returned locale SHALL be `Optional.of(Locale.forLanguageTag("en-US"))` — the
  precedence rule "explicit `LOCALE_KEY` wins over the captured header" from the parent
  requirement continues to hold post-fix
