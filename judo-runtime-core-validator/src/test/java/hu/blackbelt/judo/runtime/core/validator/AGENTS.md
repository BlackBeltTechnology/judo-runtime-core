# AGENTS.md — `judo-runtime-core-validator/src/test/java/hu/blackbelt/judo/runtime/core/validator`

| File | Purpose |
| --- | --- |
| `JarSkillPackageTest.java` | Tests classpath availability and content filtering of packaged Claude skills and agent docs. Verifies `/claude/marketplace.json` exists with `${project.version}` maven token filtered, checks `/claude/plugins/judo-validator/` metadata and SKILL.md resources, and ensures `/agent-docs/` contains mermaid diagrams in `architecture.md`. |
