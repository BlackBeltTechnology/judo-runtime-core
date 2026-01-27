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

## Approach

1. **Reference Implementation**: Complete dispatcher module with full skill package
2. **Automation Skill**: Created `/add-module-skills` to guide adding skills to any module
3. **Documentation Standards**: Use mermaid diagrams for better LLM comprehension
4. **Testing**: JUnit tests to verify JAR contains expected files
5. **Full Rollout**: Apply to all applicable modules

## Target Modules

### High Value - Full Skills (3-5 skills each)
| Module | Skills | Status |
|--------|--------|--------|
| `dispatcher` | create-interceptor, dispatcher-architecture, debug-operations | Done |
| `dao-rdbms` | custom-queries, query-debugging, dialect-extension | Done |
| `expression` | expression-syntax, custom-functions | Done |
| `validator` | custom-validators, validation-rules | Done |

### Medium Value - Some Skills (1-2 skills each)
| Module | Skills | Status |
|--------|--------|--------|
| `guice` | module-setup, dependency-injection | Pending |
| `spring` | autoconfiguration, spring-integration | Pending |
| `security` | authentication-flow, custom-auth | Pending |
| `query` | query-translation, query-optimization | Pending |
| `dao-core` | dao-patterns, entity-mapping | Pending |
| `accessmanager` | access-control, permission-checking | Pending |
| `accessmanager-api` | access-api-overview | Pending |
| `jackson` | serialization-config | Pending |
| `jaxrs` | rest-endpoints | Pending |
| `jaxrs-cxf` | cxf-integration | Pending |

### Low Value - Agent-Docs Only (no skills)
| Module | Status |
|--------|--------|
| `dao-rdbms-hsqldb` | Pending |
| `dao-rdbms-postgresql` | Pending |
| `dao-rdbms-liquibase` | Pending |
| `guice-hsqldb` | Pending |
| `guice-postgresql` | Pending |
| `guice-jetty` | Pending |
| `guice-cxf` | Pending |
| `guice-keycloak` | Pending |
| `guice-testkit` | Pending |
| `spring-hsqldb` | Pending |
| `spring-postgresql` | Pending |
| `security-keycloak` | Pending |
| `security-keycloak-cxf` | Pending |
| `jaxrs-cxf-server` | Pending |
| `export-jxls` | Pending |

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
