# Include Plugin Skills in JAR

## Summary

Add Maven resources plugin configuration to all submodules to package `.claude/plugins` and `.claude/marketplace.json` skill files into their JARs, making them accessible at runtime via classpath.

## Motivation

Skill files (AI prompts, workflow definitions, interceptor configurations) stored in `.claude/plugins/` need to be available at runtime for:
- Loading AI prompt templates dynamically
- Reading workflow configuration files
- Accessing plugin metadata and skill definitions

Currently these files exist only in the source tree and are not included in the built artifacts.

## Scope

### In Scope
- Add maven-resources-plugin execution to all 30 submodules that produce JARs
- Copy `.claude/plugins/**` and `.claude/marketplace.json` to JAR
- Target output directory: `claude/` in classpath (preserving directory structure)

### Out of Scope
- Runtime loading utilities (can be added separately)
- Plugin discovery mechanisms
- POM-only modules (`judo-runtime-core-dependencies`)

## Approach

Add a maven-resources-plugin execution in each submodule's pom.xml that copies files from the root `.claude` directory to `${project.build.outputDirectory}/claude/` during the `process-resources` phase.

Configuration template (already implemented in `judo-runtime-core-guice-testkit`):
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-resources-plugin</artifactId>
    <version>3.3.0</version>
    <executions>
        <execution>
            <id>copy-claude-plugins</id>
            <phase>process-resources</phase>
            <goals>
                <goal>copy-resources</goal>
            </goals>
            <configuration>
                <outputDirectory>${project.build.outputDirectory}/claude</outputDirectory>
                <resources>
                    <resource>
                        <directory>${maven.multiModuleProjectDirectory}/.claude</directory>
                        <filtering>false</filtering>
                        <includes>
                            <include>marketplace.json</include>
                            <include>plugins/**</include>
                        </includes>
                    </resource>
                </resources>
            </configuration>
        </execution>
    </executions>
</plugin>
```

## Submodules to Update

The following 30 modules require the maven-resources-plugin configuration:

### Core Modules
1. `judo-runtime-core` - Core runtime abstractions
2. `judo-runtime-core-dao-core` - Core DAO interfaces
3. `judo-runtime-core-dao-rdbms` - RDBMS DAO implementations
4. `judo-runtime-core-dao-rdbms-hsqldb` - HSQLDB dialect
5. `judo-runtime-core-dao-rdbms-postgresql` - PostgreSQL dialect
6. `judo-runtime-core-dao-rdbms-liquibase` - Liquibase integration

### Business Logic Modules
7. `judo-runtime-core-expression` - Expression evaluation
8. `judo-runtime-core-query` - Query processing
9. `judo-runtime-core-dispatcher` - Request dispatching
10. `judo-runtime-core-validator` - Data validation

### Security Modules
11. `judo-runtime-core-security` - Core security framework
12. `judo-runtime-core-security-keycloak` - Keycloak provider
13. `judo-runtime-core-security-keycloak-cxf` - Keycloak CXF integration
14. `judo-runtime-core-accessmanager-api` - Access control API
15. `judo-runtime-core-accessmanager` - Access control implementation

### Serialization & API Modules
16. `judo-runtime-core-jackson` - Jackson JSON providers
17. `judo-runtime-core-jaxrs` - JAX-RS providers
18. `judo-runtime-core-jaxrs-cxf` - CXF JAX-RS integration
19. `judo-runtime-core-jaxrs-cxf-server` - CXF server config

### Guice Integration Modules
20. `judo-runtime-core-guice` - Core Guice modules
21. `judo-runtime-core-guice-hsqldb` - Guice + HSQLDB
22. `judo-runtime-core-guice-postgresql` - Guice + PostgreSQL
23. `judo-runtime-core-guice-jetty` - Guice + Jetty
24. `judo-runtime-core-guice-cxf` - Guice + CXF
25. `judo-runtime-core-guice-keycloak` - Guice + Keycloak
26. `judo-runtime-core-guice-testkit` - Testing toolkit (ALREADY DONE)

### Spring Integration Modules
27. `judo-runtime-core-spring` - Spring Boot autoconfiguration
28. `judo-runtime-core-spring-hsqldb` - Spring + HSQLDB
29. `judo-runtime-core-spring-postgresql` - Spring + PostgreSQL

### Utility Modules
30. `judo-runtime-core-export-jxls` - Excel export

## Files Affected

- 29 submodule `pom.xml` files (testkit already done)
