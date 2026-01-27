# Tasks

## Implementation

- [x] Add maven-resources-plugin configuration to parent pom.xml
  - Plugin: `org.apache.maven.plugins:maven-resources-plugin`
  - Phase: `process-resources`
  - Source: `${basedir}/.claude/plugins`
  - Target: `${project.build.outputDirectory}/claude-plugins`
  - Filtering: `false` (preserve files as-is)

## Verification

- [x] Build project and verify files are copied to target/classes/claude-plugins/
- [ ] Verify JAR contains claude-plugins/ directory with skill files
