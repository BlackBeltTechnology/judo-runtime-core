# Tasks

## Phase 1: Documentation Cleanup

- [x] Replace ASCII diagrams with mermaid in `docs/submodule-agent-docs.md`
- [x] Replace ASCII diagrams with mermaid in `docs/integration_plan_to_n8n.md`
- [x] Replace ASCII diagrams with mermaid in `docs/include_skill_files_in_jar.md`

## Phase 2: Dispatcher Module - Skill Package Structure

- [x] Create `judo-runtime-core-dispatcher/src/main/resources/claude/` directory structure
- [x] Create `claude/marketplace.json` with version placeholder
- [x] Create `claude/INSTALL.md` with extraction instructions
- [x] Create `claude/plugins/judo-dispatcher/.claude-plugin/plugin.json`
- [x] Create `claude/plugins/judo-dispatcher/skills/create-interceptor/SKILL.md`
- [x] Create `claude/plugins/judo-dispatcher/skills/dispatcher-architecture/SKILL.md`
- [x] Create `claude/plugins/judo-dispatcher/skills/debug-operations/SKILL.md`

## Phase 3: Dispatcher Module - Agent Docs

- [x] Create `judo-runtime-core-dispatcher/agent-docs/` directory (at submodule root)
- [x] Create `agent-docs/README.md` - module overview
- [x] Create `agent-docs/architecture.md` - internal architecture with mermaid diagrams
- [x] Create `agent-docs/extension-points.md` - all extension interfaces
- [x] Create `agent-docs/examples/webhook-interceptor.java` - complete example

## Phase 4: Maven Configuration

- [x] Update `judo-runtime-core-dispatcher/pom.xml` with resource filtering
  - Filter `marketplace.json` and `plugin.json` for version substitution
  - Copy other files without filtering
  - Add maven-resources-plugin execution to copy agent-docs from submodule root

## Phase 5: JUnit Tests

- [x] Create `JarSkillPackageTest.java` in dispatcher module
  - Test JAR contains `claude/marketplace.json`
  - Test version is substituted (no `${project.version}`)
  - Test skill files exist
  - Test agent-docs exist

## Phase 6: Build Verification

- [x] Run `mvn clean package -pl judo-runtime-core-dispatcher -DskipTests`
- [x] Verify JAR structure with `jar tf`
- [x] Run JUnit test to validate structure
- [x] Test classpath access to resources

## Phase 7: Template Documentation

- [x] Update `docs/submodule-agent-docs.md` with final patterns
- [x] Create checklist for adding skills to other modules
- [x] Document testing approach

## Dependencies

```mermaid
graph LR
    P1[Phase 1: Doc Cleanup] --> P2[Phase 2: Skills]
    P2 --> P3[Phase 3: Agent Docs]
    P3 --> P4[Phase 4: Maven]
    P4 --> P5[Phase 5: Tests]
    P5 --> P6[Phase 6: Verify]
    P6 --> P7[Phase 7: Templates]
```

## Phase 8: Restructure Agent Docs Location

Move agent-docs from `src/main/resources/agent-docs/` to submodule root and update pom.xml files.

- [ ] Move agent-docs to submodule root for all high-value modules (dispatcher, dao-rdbms, expression, validator)
- [ ] Move agent-docs to submodule root for all medium-value modules (10 modules)
- [ ] Move agent-docs to submodule root for all low-value modules (15 modules)
- [ ] Add maven-resources-plugin configuration to each submodule pom.xml
- [ ] Verify build still works and JAR contains agent-docs

## Notes

- Start with dispatcher as reference implementation
- Other modules can follow the same pattern
- JUnit tests ensure structure stays valid across builds
- Mermaid diagrams are preferred for all documentation
- **agent-docs/ lives at submodule root, NOT in src/main/resources**
