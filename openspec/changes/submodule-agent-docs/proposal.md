# Submodule Agent Docs & Skills Architecture

## Summary

Make each JUDO Runtime Core submodule a self-contained skill package with embedded documentation, marketplace definitions, and installation instructions - all packaged into the JAR and discoverable at runtime.

## Motivation

Currently:
- Skills and agent-docs exist only for `judo-runtime-core-guice-testkit`
- Other modules have no embedded documentation for AI agents
- Consumers have no way to discover available skills from their dependencies
- No standardized structure for module documentation

With this change:
- Each module becomes a self-contained skill package
- Documentation travels with the JAR
- Consumers can extract and install skills from any dependency
- Standardized structure enables tooling and automation

## Scope

### In Scope
- Define standard structure for `claude/` and `agent-docs/` in each module
- Implement dispatcher module as complete reference example
- Create `/add-module-skills` skill to automate the process
- **Apply skill packages to ALL applicable modules** (not just dispatcher)
- Create Maven configuration for resource packaging with version substitution
- Create INSTALL.md template for skill extraction from JARs
- Replace ASCII diagrams with mermaid in all documentation
- Add JUnit tests to validate JAR contents

### Out of Scope
- Automated skill generation from Javadoc (future enhancement)
- CLI tooling for skill installation (future enhancement)
- BOM/dependency-only modules (no code to document)

## Directory Structure

### Source Layout (in each submodule)
```
judo-runtime-core-<module>/
├── agent-docs/                    # At submodule root (NOT in src/main/resources)
│   ├── README.md                  # Module overview and documentation
│   ├── architecture.md            # (optional) Architecture details
│   └── ...                        # Additional documentation files
├── src/main/resources/
│   └── claude/                    # Skills and marketplace config
│       ├── marketplace.json       # Package metadata (version filtered)
│       ├── INSTALL.md             # Installation instructions
│       └── plugins/<plugin-name>/ # Plugin with skills
│           ├── .claude-plugin/
│           │   └── plugin.json    # Plugin metadata (version filtered)
│           └── skills/<skill>/
│               └── SKILL.md       # Skill documentation
└── pom.xml                        # Includes maven-resources-plugin config
```

### JAR Layout (after build)
```
<module>.jar
├── agent-docs/                    # Copied from submodule root
│   └── README.md
├── claude/                        # From src/main/resources
│   ├── marketplace.json           # Version substituted
│   ├── INSTALL.md
│   └── plugins/...
└── hu/blackbelt/...               # Java classes
```

### Maven Configuration

Each submodule's pom.xml includes maven-resources-plugin to copy agent-docs from the submodule root:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-resources-plugin</artifactId>
    <version>3.3.0</version>
    <executions>
        <execution>
            <id>copy-agent-docs</id>
            <phase>process-resources</phase>
            <goals>
                <goal>copy-resources</goal>
            </goals>
            <configuration>
                <outputDirectory>${project.build.outputDirectory}/agent-docs</outputDirectory>
                <resources>
                    <resource>
                        <directory>${basedir}/agent-docs</directory>
                        <filtering>false</filtering>
                    </resource>
                </resources>
            </configuration>
        </execution>
    </executions>
</plugin>
```

## Approach

1. **Reference Implementation**: Complete dispatcher module with full skill package
2. **Automation Skill**: Created `/add-module-skills` to guide adding skills to any module
3. **Documentation Standards**: Use mermaid diagrams for better LLM comprehension
4. **Testing**: JUnit tests to verify JAR contains expected files
5. **Full Rollout**: Apply to all applicable modules

## Skill Naming Convention

All skills use the `judo-runtime:` prefix for namespace clarity:
- `judo-runtime:create-interceptor`
- `judo-runtime:dispatcher-architecture`
- `judo-runtime:debug-operations`
- etc.

## Target Modules

### High Value - Full Skills (3-5 skills each)
| Module | Skills | Status |
|--------|--------|--------|
| `dispatcher` | judo-runtime:create-interceptor, judo-runtime:dispatcher-architecture, judo-runtime:debug-operations | Done |
| `dao-rdbms` | judo-runtime:custom-queries, judo-runtime:query-debugging, judo-runtime:dialect-extension | Done |
| `expression` | judo-runtime:expression-syntax, judo-runtime:custom-functions | Done |
| `validator` | judo-runtime:custom-validators, judo-runtime:validation-rules | Done |

### Medium Value - Some Skills (1-2 skills each)
| Module | Skills | Status |
|--------|--------|--------|
| `guice` | judo-runtime:module-setup, judo-runtime:dependency-injection | Done |
| `spring` | judo-runtime:autoconfiguration, judo-runtime:spring-integration | Done |
| `security` | judo-runtime:authentication-flow, judo-runtime:custom-auth | Done |
| `query` | judo-runtime:query-translation, judo-runtime:query-optimization | Done |
| `dao-core` | judo-runtime:dao-patterns, judo-runtime:entity-mapping | Done |
| `accessmanager` | judo-runtime:access-control, judo-runtime:permission-checking | Done |
| `accessmanager-api` | judo-runtime:access-api-overview | Done |
| `jackson` | judo-runtime:serialization-config | Done |
| `jaxrs` | judo-runtime:rest-endpoints | Done |
| `jaxrs-cxf` | judo-runtime:cxf-integration | Done |

### Low Value - Agent-Docs Only (no skills)
| Module | Status |
|--------|--------|
| `dao-rdbms-hsqldb` | Done |
| `dao-rdbms-postgresql` | Done |
| `dao-rdbms-liquibase` | Done |
| `guice-hsqldb` | Done |
| `guice-postgresql` | Done |
| `guice-jetty` | Done |
| `guice-cxf` | Done |
| `guice-keycloak` | Done |
| `guice-testkit` | Done |
| `spring-hsqldb` | Done |
| `spring-postgresql` | Done |
| `security-keycloak` | Done |
| `security-keycloak-cxf` | Done |
| `jaxrs-cxf-server` | Done |
| `export-jxls` | Done |

### No Skills - BOM Only
| Module | Reason |
|--------|--------|
| `dependencies` | BOM only, no code |
| `guice-dependencies` | BOM only, no code |

## Success Criteria

- All applicable modules contain `claude/` and `agent-docs/` directories
- `marketplace.json` has correct version from Maven (via filtering)
- **SKILL.md files include YAML frontmatter** following Agent Skills standard
- Skills are accessible via classpath at runtime
- JUnit tests validate JAR structure for each module
- Documentation uses mermaid diagrams throughout
- `/add-module-skills` skill available for future modules
