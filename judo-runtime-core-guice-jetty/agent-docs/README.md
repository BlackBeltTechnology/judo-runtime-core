# JUDO Guice Jetty Module

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-guice-jetty
- **Version**: ${project.version}

## Overview

This module provides Google Guice integration for Eclipse Jetty, enabling embedded HTTP server functionality with configurable thread pools and servlet container support.

## Main Components

### JudoJettyModule

The primary Guice module that configures the Jetty servlet container.

```java
import hu.blackbelt.judo.runtime.core.jetty.guice.JudoJettyModule;

// Basic usage with defaults
Injector injector = Guice.createInjector(
    JudoJettyModule.builder().build()
);

// Custom configuration
Injector injector = Guice.createInjector(
    JudoJettyModule.builder()
        .jettyServerPort(8080)
        .jettyContextPath("/api")
        .maxThreads(200)
        .minThreads(20)
        .idleTimeout(300)
        .build()
);
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `jettyServerPort` | Integer | `8181` | HTTP server port |
| `jettyContextPath` | String | `"/"` | Context path for the application |
| `maxThreads` | Integer | `100` | Maximum thread pool size |
| `minThreads` | Integer | `10` | Minimum thread pool size |
| `idleTimeout` | Integer | `120` | Thread idle timeout in seconds |

### Provided Bindings

The module binds the following types:

| Type | Implementation | Scope |
|------|----------------|-------|
| `JettyContainer` | Container with embedded server | Eager Singleton |

### JettyContainer

The `JettyContainer` class wraps the Jetty server and provides lifecycle management:

```java
@Inject
private JettyContainer jettyContainer;

// The server starts automatically as an eager singleton
// Access the underlying server if needed
Server server = jettyContainer.getServer();
```

## Maven Dependency

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-guice-jetty</artifactId>
    <version>${project.version}</version>
</dependency>
```

## Usage with CXF Module

Typically used alongside the CXF module for REST API support:

```java
Injector injector = Guice.createInjector(
    JudoJettyModule.builder()
        .jettyServerPort(8080)
        .jettyContextPath("/")
        .maxThreads(150)
        .build(),
    JudoCxfModule.builder()
        .cxfJaxRsServerPath("api")
        .build()
);
```

## Configuration Qualifiers

Inject configuration values using qualifier annotations:

```java
import hu.blackbelt.judo.runtime.core.jetty.guice.JettyConfigurations;

@Inject
@JettyConfigurations.JettyServerPort
private Integer serverPort;

@Inject
@JettyConfigurations.JettyServerContextPath
private String contextPath;

@Inject
@JettyConfigurations.JettyServerMaxThreads
private Integer maxThreads;
```

## Thread Pool Sizing

Guidelines for thread pool configuration:

- **Development**: `minThreads=10`, `maxThreads=100`
- **Production (light load)**: `minThreads=20`, `maxThreads=200`
- **Production (heavy load)**: `minThreads=50`, `maxThreads=500`
- Adjust `idleTimeout` based on expected request patterns
