## ADDED Requirements

### Requirement: `DefaultDispatcher.callOperation` propagates `LOCALE_KEY` only from the exchange

`DefaultDispatcher.callOperation` SHALL propagate `Context[DefaultDispatcher.LOCALE_KEY]` from
the operation exchange **only when the exchange carries `LOCALE_KEY`**. When the exchange has no
`LOCALE_KEY`, `Context[LOCALE_KEY]` SHALL remain absent — `callOperation` SHALL NOT synthesise a
value from a JVM default, a builder-provided fallback, or any other source.

This mirrors the sibling `ACTOR_KEY` propagation in the same method and ensures the anonymous
locale-resolution precedence walk defined by `anonymous-request-locale` (Context `LOCALE_KEY` →
`RequestLocaleHolder` → `defaultLanguage`) is reachable end-to-end through a dispatched
operation.

> Change note: closes the shadowing bug documented in
> `docs/JNG-6415-dispatcher-locale-key-shadowing.md`. The `defaultLocale` builder parameter,
> field, and JVM-default initialisation on `DefaultDispatcher` are removed as part of this
> requirement — they had zero in-tree callers and existed only to feed the removed
> always-write path.

#### Scenario: Exchange without `LOCALE_KEY` leaves the context slot absent
- **WHEN** `DefaultDispatcher.callOperation` is invoked with an exchange whose keys do not
  include `DefaultDispatcher.LOCALE_KEY`
- **THEN** inside the dispatched operation body, `context.getAs(Locale.class,
  DefaultDispatcher.LOCALE_KEY)` SHALL return `null`

#### Scenario: Exchange with `LOCALE_KEY` populates the context slot
- **WHEN** `DefaultDispatcher.callOperation` is invoked with an exchange containing
  `LOCALE_KEY = Locale.forLanguageTag("hu-HU")`
- **THEN** inside the dispatched operation body, `context.getAs(Locale.class,
  DefaultDispatcher.LOCALE_KEY)` SHALL return `Locale.forLanguageTag("hu-HU")`

#### Scenario: No JVM-default fallback is written
- **WHEN** `DefaultDispatcher.callOperation` is invoked with an exchange that carries no
  `LOCALE_KEY`, on a JVM whose `Locale.getDefault()` is `Locale.ENGLISH`
- **THEN** inside the dispatched operation body, `context.getAs(Locale.class,
  DefaultDispatcher.LOCALE_KEY)` SHALL NOT return `Locale.ENGLISH` — it SHALL return `null`
