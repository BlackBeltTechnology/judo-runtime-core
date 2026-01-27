package hu.blackbelt.judo.runtime.core.validator;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests that verify the Claude skill package is properly included in the JAR
 * and accessible via classpath.
 */
class JarSkillPackageTest {

    @Test
    void marketplaceJsonShouldBeAccessibleViaClasspath() {
        InputStream is = getClass().getResourceAsStream("/claude/marketplace.json");
        assertThat("marketplace.json should be accessible via classpath", is, notNullValue());
    }

    @Test
    void marketplaceJsonShouldHaveVersionSubstituted() {
        String content = readResource("/claude/marketplace.json");
        
        assertThat("marketplace.json should not contain unsubstituted placeholder",
            content, not(containsString("${project.version}")));
        
        assertThat("marketplace.json should contain module name",
            content, containsString("judo-runtime-core-validator"));
    }

    @Test
    void pluginJsonShouldBeAccessibleViaClasspath() {
        InputStream is = getClass().getResourceAsStream(
            "/claude/plugins/judo-validator/.claude-plugin/plugin.json");
        assertThat("plugin.json should be accessible via classpath", is, notNullValue());
    }

    @Test
    void customValidatorsSkillShouldBeAccessible() {
        InputStream is = getClass().getResourceAsStream(
            "/claude/plugins/judo-validator/skills/custom-validators/SKILL.md");
        assertThat("custom-validators skill should be accessible", is, notNullValue());
    }

    @Test
    void validationRulesSkillShouldBeAccessible() {
        InputStream is = getClass().getResourceAsStream(
            "/claude/plugins/judo-validator/skills/validation-rules/SKILL.md");
        assertThat("validation-rules skill should be accessible", is, notNullValue());
    }

    @Test
    void installMdShouldBeAccessible() {
        InputStream is = getClass().getResourceAsStream("/claude/INSTALL.md");
        assertThat("INSTALL.md should be accessible via classpath", is, notNullValue());
    }

    @Test
    void agentDocsReadmeShouldBeAccessible() {
        InputStream is = getClass().getResourceAsStream("/agent-docs/README.md");
        assertThat("agent-docs/README.md should be accessible", is, notNullValue());
    }

    @Test
    void agentDocsArchitectureShouldBeAccessible() {
        InputStream is = getClass().getResourceAsStream("/agent-docs/architecture.md");
        assertThat("agent-docs/architecture.md should be accessible", is, notNullValue());
        
        String content = readResource("/agent-docs/architecture.md");
        assertThat("architecture.md should contain mermaid diagrams",
            content, containsString("```mermaid"));
    }

    @Test
    void agentDocsExtensionPointsShouldBeAccessible() {
        InputStream is = getClass().getResourceAsStream("/agent-docs/extension-points.md");
        assertThat("agent-docs/extension-points.md should be accessible", is, notNullValue());
    }

    private String readResource(String path) {
        try (InputStream is = getClass().getResourceAsStream(path);
             BufferedReader reader = new BufferedReader(
                 new InputStreamReader(is, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to read resource: " + path, e);
        }
    }
}
