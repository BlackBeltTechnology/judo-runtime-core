# Include Plugin Skills in JAR

## Summary

Add Maven resources plugin configuration to package `.claude/plugins` skill files into the application JAR, making them accessible at runtime via classpath.

## Motivation

Skill files (AI prompts, workflow definitions, interceptor configurations) stored in `.claude/plugins/` need to be available at runtime for:
- Loading AI prompt templates dynamically
- Reading workflow configuration files
- Accessing plugin metadata and skill definitions

Currently these files exist only in the source tree and are not included in the built artifacts.

## Scope

### In Scope
- Add maven-resources-plugin execution to copy `.claude/plugins` to JAR
- Target output directory: `claude-plugins/` in classpath

### Out of Scope
- Runtime loading utilities (can be added separately)
- Plugin discovery mechanisms

## Approach

Add a maven-resources-plugin execution in the parent POM that copies files from `.claude/plugins/` to `${project.build.outputDirectory}/claude-plugins/` during the `process-resources` phase.

## Files Affected

- `pom.xml` (parent) - Add plugin configuration in `<pluginManagement>` or directly
