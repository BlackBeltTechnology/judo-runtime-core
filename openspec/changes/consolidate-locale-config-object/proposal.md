## Why

`JNG-6415` (`add-principal-locale-resolution` + `add-anonymous-request-locale`) introduced four
platform locale settings — `principalLocaleAttribute`, `supportedLanguages`, `defaultLanguage`,
`browserLanguageCheck` — and wired them through the runtime as **four independent scalar
parameters**. Verified against the tree, the same four values are now:

- passed as four separate `@Builder` fields into `DefaultActorResolver` (`DefaultActorResolver.java:73-108`);
- passed as three separate ctor parameters into `PrincipalLocaleProvider`
  (`PrincipalLocaleProvider.java:52-70`) — which itself re-parses `supportedLanguages` on every
  construction;
- injected via **four dedicated Guice `@BindingAnnotation` qualifiers**
  (`JudoConfigurationQualifiers.ActorResolver{PrincipalLocaleAttribute,SupportedLanguages,DefaultLanguage,BrowserLanguageCheck}`)
  at two sites (`DefaultActorResolverProvider`, `PrincipalLocaleProviderProvider`);
- rebound as four instance bindings in `JudoDefaultModule.configureConfiguration`;
- reconfigured through four `@Value("${judo.platform.…}")` parameters on **two** Spring `@Bean`
  methods in `JudoDefaultSpringConfiguration` (`getActorResolver`, `getLocaleProvider`) — the same
  three values duplicated.

That is one logical configuration bundle expressed 3× (Guice qualifiers, Guice module, Spring
config) and consumed 2× (resolver, provider). Adding a fifth locale setting today requires editing
5 files.

## What Changes

- Introduce an immutable value type `PrincipalLocaleConfig` in `judo-runtime-core-security`
  (co-located with `PrincipalLocaleResolver`, the pure helper it exists to feed), Lombok
  `@Value @Builder`, four fields: `principalLocaleAttribute`, `supportedLanguages` (raw CSV),
  `defaultLanguage`, `browserLanguageCheck` (default `true`). Exposes a `@Getter(lazy = true)`
  `Set<String> parsedSupportedLanguages` (computed once via
  `PrincipalLocaleResolver.parseSupportedLanguages`) and a convenience `isEnabled()` returning
  `principalLocaleAttribute != null && !blank`.
- Collapse `DefaultActorResolver.builder()`'s four locale fields into a single `localeConfig`
  field. Backwards compatibility with in-tree callers is preserved by the Guice/Spring providers;
  no legacy adapter methods are added on the builder itself (any direct downstream users migrate
  to `.localeConfig(...)`).
- Collapse `PrincipalLocaleProvider`'s ctor from `(context, attr, supported, default)` to
  `(context, localeConfig)`. Reads switch to `localeConfig.getParsedSupportedLanguages()`, so the
  supported-set is parsed once at wiring time instead of on every `PrincipalLocaleProvider`
  construction.
- Guice: `DefaultActorResolverProvider` and `PrincipalLocaleProviderProvider` each replace 3–4
  qualified string/boolean `@Inject`s with a single `@Inject(optional=true) @Nullable
  PrincipalLocaleConfig localeConfig`. `JudoDefaultModule.configureConfiguration` binds
  `PrincipalLocaleConfig` once (built from the same `JudoDefaultModuleConfiguration` fields it
  reads today; those fields are the public app-facing surface and are kept). The four now-unused
  qualifier annotations
  (`ActorResolver{PrincipalLocaleAttribute,SupportedLanguages,DefaultLanguage,BrowserLanguageCheck}`)
  are **deleted** — they were introduced by `JNG-6415` and have no downstream consumers outside
  the two providers this change edits.
- Spring: introduce a single `@Bean PrincipalLocaleConfig` that reads the four
  `judo.platform.*` properties. `getActorResolver` and `getLocaleProvider` take that bean as an
  argument instead of duplicating `@Value`s. Property names, defaults, and app-facing surface are
  unchanged.
- Tests migrate their SUT construction from loose scalars to `PrincipalLocaleConfig.builder()`.
  No scenario is added, removed, or altered — this is a mechanical refactor whose contract is
  "behaviour and precedence identical to `JNG-6415`."

## Impact

- **judo-runtime-core-security**: adds `PrincipalLocaleConfig` (value object). No behaviour.
- **judo-runtime-core-dispatcher**: `DefaultActorResolver` builder loses 4 fields, gains 1;
  `PrincipalLocaleProvider` ctor loses 3 args, gains 1. `parseSupportedLanguages` no longer runs
  per provider construction.
- **judo-runtime-core-guice**: `DefaultActorResolverProvider` and `PrincipalLocaleProviderProvider`
  simplified; `JudoDefaultModule.configureConfiguration` binds `PrincipalLocaleConfig` once; four
  `@BindingAnnotation` qualifiers deleted from `JudoConfigurationQualifiers`.
- **judo-runtime-core-spring**: `JudoDefaultSpringConfiguration` gains one `@Bean`, loses seven
  `@Value` parameters across two beans.
- **Backwards compatibility**: the app-facing surface — `JudoDefaultModuleConfiguration.builder()`
  and the `judo.platform.*` Spring property names — is unchanged. Defaults are unchanged. Feature
  gate (blank `principalLocaleAttribute` ⇒ feature off) is unchanged. Precedence and refresh
  semantics from `JNG-6415` are unchanged.
- **Breaking**: direct construction of `DefaultActorResolver.builder()` or
  `PrincipalLocaleProvider` outside the runtime-core Guice/Spring providers must switch to the
  new signatures. Both types are internal wiring surface, not documented app extension points, so
  the blast radius is expected to be limited to this repo's tests.
- **No metamodel, DAO, dispatcher-API, or JAX-RS surface change.**

## Capabilities

### New Capabilities
_None._

### Modified Capabilities
- `principal-locale-resolution` — wiring requirement clarifies that runtime-core exposes locale
  configuration to consumers as a single value object (`PrincipalLocaleConfig`) rather than as
  four independent scalar bindings. The four platform property/env-var names, their defaults, and
  every observable behaviour (precedence, refresh, feature-off gate, `LocaleProvider`
  registration) are unchanged.
