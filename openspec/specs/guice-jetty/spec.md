# Guice Jetty Module Specification

## Purpose
Provides a Google Guice module (`JudoJettyModule`) that configures and manages an embedded Jetty servlet container for hosting JUDO REST/JAX-RS services, with configurable port, context path, and thread pool settings.

## Architecture
- **JudoJettyModule** (`hu.blackbelt.judo.runtime.core.jetty.guice.JudoJettyModule`): `AbstractModule` that binds Jetty configuration options and the `JettyContainer` as an eager singleton.
- **JudoJettyModuleConfiguration** (nested in `JudoJettyModule`): Lombok `@Builder` configuration with defaults (port 8181, context path "/", maxThreads 100, minThreads 10, idleTimeout 120).
- **JettyConfigurations** (`hu.blackbelt.judo.runtime.core.jetty.guice.JettyConfigurations`): Binding annotations for Jetty configuration (`@JettyServerPort`, `@JettyServerContextPath`, `@JettyServerMaxThreads`, `@JettyServerMinThreads`, `@JettyServerIdleTimeout`).
- **JettyContainer** (`hu.blackbelt.judo.runtime.core.jetty.guice.JettyContainer`): Manages the Jetty `Server` lifecycle (start/stop), configures `QueuedThreadPool`, `ServerConnector`, and `ServletContextHandler` with session support.
- **JettyContainerProvider** (`hu.blackbelt.judo.runtime.core.jetty.guice.JettyContainerProvider`): Guice `Provider` for `JettyContainer`.

## Requirements

### Requirement: Jetty Server Lifecycle Management
`JettyContainer` SHALL manage the full lifecycle of an embedded Jetty server, starting it on construction and providing a `stop()` method for shutdown.

#### Scenario: Server starts on construction
- **GIVEN** a `JettyContainer` constructed with port 8181 and context path "/"
- **WHEN** the constructor completes
- **THEN** a Jetty `Server` SHALL be running and listening on port 8181

#### Scenario: Server uses fallback port when configured port is invalid
- **GIVEN** a `JettyContainer` with port set to 0 or negative
- **WHEN** `start()` is called
- **THEN** the server SHALL fall back to port 8080

#### Scenario: Server stops gracefully
- **GIVEN** a running `JettyContainer`
- **WHEN** `stop()` is called
- **THEN** the Jetty server SHALL be stopped and `isStopped()` SHALL return true

### Requirement: Thread Pool Configuration
`JettyContainer` SHALL configure the Jetty `QueuedThreadPool` with the provided max threads, min threads, and idle timeout values.

#### Scenario: Custom thread pool settings
- **GIVEN** a `JudoJettyModule` built with `maxThreads(200)`, `minThreads(20)`, `idleTimeout(60)`
- **WHEN** the `JettyContainer` is created
- **THEN** the Jetty `QueuedThreadPool` SHALL be initialized with maxThreads=200, minThreads=20, idleTimeout=60

### Requirement: Servlet Context Handler with Session Support
`JettyContainer` SHALL configure a `ServletContextHandler` with session handling enabled.

#### Scenario: Context handler is configured
- **GIVEN** a `JettyContainer` with contextPath "/api"
- **WHEN** the server starts
- **THEN** a `ServletContextHandler` SHALL be created with `SESSIONS` mode, the context path set to "/api", and a `SessionHandler` attached

### Requirement: Configuration Binding
The module SHALL bind all Jetty configuration parameters via qualified annotations.

#### Scenario: Default configuration values are bound
- **GIVEN** a `JudoJettyModule` built with default configuration
- **WHEN** the injector is created
- **THEN** `@JettyServerPort` SHALL resolve to `8181`, `@JettyServerContextPath` to `"/"`, `@JettyServerMaxThreads` to `100`, `@JettyServerMinThreads` to `10`, `@JettyServerIdleTimeout` to `120`

### Requirement: Eager Singleton Binding
The module SHALL bind `JettyContainer` as an eager singleton so the server starts immediately on injector creation.

#### Scenario: JettyContainer is eagerly initialized
- **GIVEN** a `JudoJettyModule` installed in a Guice injector
- **WHEN** the injector is created
- **THEN** `JettyContainer` SHALL be instantiated and the Jetty server SHALL be running
