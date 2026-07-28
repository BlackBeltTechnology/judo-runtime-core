## 1. Generalize the request-aware LocaleProvider (TDD)

- [x] 1.1 Extended `PrincipalLocaleProviderTest` (dispatcher) with anonymous-branch cases: no principal + supported `Accept-Language` → that tag; language-only range → region tag; unsupported → default; no header → default; unsupported+blank default → empty; app-set `LOCALE_KEY` wins; authenticated principal still wins over browser. 15 tests total.
- [x] 1.2 Added a `supportedLanguages` field to `PrincipalLocaleProvider` (parsed set) and an anonymous branch resolving via `PrincipalLocaleResolver.matchSupportedLanguage(acceptLanguage, supportedLanguages)` (new public browser-tier helper). Constructor now `(Context, principalLocaleAttribute, supportedLanguages, defaultLanguage)`.
- [x] 1.3 Authenticated path preserved exactly (actor payload → principal attributes → default); anonymous branch runs only when `isPrincipalBound()` is false.
- [x] 1.4 App-set `LOCALE_KEY` (resolved `Locale`) honored as highest precedence for the anonymous case.
- [x] 1.5 Tests green; `mvn -pl judo-runtime-core-dispatcher test`.

## 2. Accept-Language capture (TDD)

> DESIGN CORRECTION: a JAX-RS filter stashing into the dispatcher `Context` is **unsound** — for
> exposed (HTTP-entry) operations `DefaultDispatcher.callOperation` calls `context.removeAll()` before
> dispatch and repopulates `LOCALE_KEY`/`ACTOR_KEY` **from the exchange**, wiping any pre-dispatch
> `Context` value before messages format. The exchange itself is built by the platform/generated REST
> layer, not runtime-core. Sound approach: a thread-local (`RequestLocaleHolder`, in the core module)
> set by a CXF transport interceptor that survives the per-operation `Context` reset.

- [x] 2.1 Added `AcceptLanguageCaptureInterceptorTest` (jaxrs-cxf): header captured (case-insensitive); absent header / no protocol headers clear the holder; `handleFault` clears. 5 tests.
- [x] 2.2 Implemented `RequestLocaleHolder` (thread-local, `judo-runtime-core` core module) and `AcceptLanguageCaptureInterceptor extends AbstractPhaseInterceptor<Message>` (RECEIVE phase, `judo-runtime-core-jaxrs-cxf`) reading `Message.PROTOCOL_HEADERS`. Provider anonymous branch reads the holder (not `Context`).
- [x] 2.3 Interceptor sets the holder on every request (null when absent) so a pooled thread never keeps a stale value; clears on fault.
- [x] 2.4 Tests green; `mvn -pl judo-runtime-core-jaxrs-cxf test`.

## 3. Wiring

- [x] 3.1 Guice: `JudoCxfModule.configureAcceptLanguageCaptureInterceptor()` registers the interceptor as a CXF in-interceptor; `PrincipalLocaleProviderProvider` now injects + passes `supportedLanguages`.
- [x] 3.2 Spring: `getLocaleProvider` bean now passes `supportedLanguages`. NOTE: runtime-core-spring has **no CXF server assembly** (that lives in the Spring Boot starter), so the capture interceptor registration for Spring is a starter concern — deferred (see §5).
- [x] 3.3 guice + spring + guice-cxf compile; full reactor green.

## 4. Build + docs

- [x] 4.1 Full reactor `mvn install` green (Java 21), Docker-gated PostgreSQL modules excluded.
- [x] 4.2 Update `docs/JNG-6415-multi-language-support.md` with an "Anonymous requests" section: request-scoped locale, no persistence, `RequestLocaleHolder` + CXF capture interceptor, `LOCALE_KEY` precedence, frontend `Accept-Language` drives anonymous backend messages
- [x] 4.3 `openspec validate add-anonymous-request-locale` → valid

## 5. Deferred (out of scope — follow-up)

- [ ] 5.1 judo-platform OSGi wiring: CXF interceptor service registration + shared `JUDO_PLATFORM_*` env-var mapping (same follow-up as `add-principal-locale-resolution`)
- [ ] 5.2 Spring Boot starter: register `AcceptLanguageCaptureInterceptor` as a CXF in-interceptor in the starter's CXF server assembly (runtime-core-spring has no CXF server)
- [ ] 5.3 Non-CXF transports (if ever needed) would require their own capture point
- [ ] 5.4 Frontend per-session/cookie memory of an anonymous locale choice (frontend concern)
