# AGENTS.md — `expression`

| File | Purpose |
| --- | --- |
| `JarSkillPackageTest.java` | Verifies the `claude/` skill package ships in the expression JAR. Asserts classpath access to `/claude/marketplace.json`, `/claude/plugins/judo-expression/.claude-plugin/plugin.json` and its `skills/*/SKILL.md`; asserts `${project.version}` placeholder substituted and module name `judo-runtime-core-expression` present. Callers renaming packaged skill resources fail the build. |