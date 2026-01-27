# Design: Submodule Agent Docs & Skills

## Architecture Overview

```mermaid
graph TB
    subgraph "Source Structure"
        SRC[src/main/resources]
        SRC --> CLAUDE[claude/]
        SRC --> DOCS[agent-docs/]
        
        CLAUDE --> MKT[marketplace.json]
        CLAUDE --> INST[INSTALL.md]
        CLAUDE --> PLUGINS[plugins/]
        
        PLUGINS --> PLUGIN[judo-dispatcher/]
        PLUGIN --> PLUGINJSON[.claude-plugin/plugin.json]
        PLUGIN --> SKILLS[skills/]
        
        SKILLS --> SKILL1[create-interceptor/SKILL.md]
        SKILLS --> SKILL2[debug-operations/SKILL.md]
        
        DOCS --> README[README.md]
        DOCS --> ARCH[architecture.md]
        DOCS --> EXT[extension-points.md]
    end
```

## Build Flow

```mermaid
flowchart LR
    subgraph "Maven Build"
        A[src/main/resources] -->|process-resources| B[target/classes]
        B -->|package| C[JAR file]
    end
    
    subgraph "Resource Filtering"
        D[marketplace.json<br/>plugin.json] -->|filter=true| E[Version substituted]
        F[SKILL.md<br/>INSTALL.md] -->|filter=false| G[Copied as-is]
    end
    
    A --> D
    A --> F
    E --> B
    G --> B
```

## JAR Structure

```mermaid
graph TB
    subgraph "judo-runtime-core-dispatcher-1.0.6.jar"
        META[META-INF/]
        CLASSES[hu/blackbelt/.../]
        
        subgraph "claude/"
            MKT2[marketplace.json]
            INST2[INSTALL.md]
            subgraph "plugins/judo-dispatcher/"
                PJ[.claude-plugin/plugin.json]
                subgraph "skills/"
                    S1[create-interceptor/SKILL.md]
                    S2[dispatcher-architecture/SKILL.md]
                    S3[debug-operations/SKILL.md]
                end
            end
        end
        
        subgraph "agent-docs/"
            R[README.md]
            AR[architecture.md]
            EP[extension-points.md]
            EX[examples/]
        end
    end
```

## Consumer Installation Flow

```mermaid
sequenceDiagram
    participant User
    participant Maven
    participant JAR
    participant Project
    
    User->>Maven: Add dependency
    Maven->>JAR: Download to ~/.m2
    User->>JAR: unzip "claude/*" "agent-docs/*"
    JAR->>Project: Extract to .claude/
    User->>Project: Skills available in Claude Code
```

## Module Categories

```mermaid
graph TB
    subgraph "High Value - Full Skills"
        D[dispatcher]
        DAO[dao-rdbms]
        EXP[expression]
        VAL[validator]
    end
    
    subgraph "Medium Value - Some Skills"
        G[guice]
        S[spring]
        SEC[security]
        Q[query]
    end
    
    subgraph "Low Value - Agent-Docs Only"
        H[dao-rdbms-hsqldb]
        P[dao-rdbms-postgresql]
        TK[guice-testkit]
    end
    
    subgraph "No Skills"
        DEP[dependencies]
        GDEP[guice-dependencies]
    end
```

## File Specifications

### marketplace.json Schema

```json
{
  "name": "judo-runtime-core-dispatcher",
  "version": "${project.version}",
  "description": "Module description",
  "groupId": "hu.blackbelt.judo.runtime",
  "artifactId": "judo-runtime-core-dispatcher",
  "repository": "https://github.com/BlackBeltTechnology/judo-runtime-core",
  "plugins": [
    {
      "name": "judo-dispatcher",
      "path": "./plugins/judo-dispatcher",
      "description": "Skills for working with JUDO Dispatcher"
    }
  ],
  "agentDocs": "./agent-docs"
}
```

### plugin.json Schema

```json
{
  "name": "judo-dispatcher",
  "version": "${project.version}",
  "description": "Skills for JUDO Dispatcher",
  "skills": [
    {
      "name": "create-interceptor",
      "path": "./skills/create-interceptor",
      "description": "Create custom operation interceptors"
    }
  ]
}
```

## Maven Configuration

```xml
<build>
    <resources>
        <!-- Standard resources without filtering -->
        <resource>
            <directory>src/main/resources</directory>
            <excludes>
                <exclude>claude/marketplace.json</exclude>
                <exclude>claude/plugins/*/.claude-plugin/plugin.json</exclude>
            </excludes>
        </resource>
        <!-- JSON files with version substitution -->
        <resource>
            <directory>src/main/resources</directory>
            <filtering>true</filtering>
            <includes>
                <include>claude/marketplace.json</include>
                <include>claude/plugins/*/.claude-plugin/plugin.json</include>
            </includes>
        </resource>
    </resources>
</build>
```

## JUnit Test Strategy

```mermaid
flowchart TB
    subgraph "Test: JAR Structure Validation"
        T1[Load JAR as ZipFile]
        T2[Assert claude/marketplace.json exists]
        T3[Assert version is substituted]
        T4[Assert skills directories exist]
        T5[Assert agent-docs exist]
    end
    
    T1 --> T2 --> T3 --> T4 --> T5
```

### Test Implementation

```java
@Test
void jarContainsClaudeSkillPackage() throws Exception {
    // Find the built JAR
    Path jarPath = findBuiltJar("judo-runtime-core-dispatcher");
    
    try (JarFile jar = new JarFile(jarPath.toFile())) {
        // Verify marketplace.json exists and has version
        JarEntry marketplace = jar.getJarEntry("claude/marketplace.json");
        assertNotNull(marketplace, "marketplace.json should exist");
        
        String content = readEntry(jar, marketplace);
        assertFalse(content.contains("${project.version}"), 
            "Version should be substituted");
        assertTrue(content.contains("1.0.6"), 
            "Should contain actual version");
        
        // Verify plugin structure
        assertNotNull(jar.getJarEntry("claude/plugins/judo-dispatcher/.claude-plugin/plugin.json"));
        assertNotNull(jar.getJarEntry("claude/plugins/judo-dispatcher/skills/create-interceptor/SKILL.md"));
        
        // Verify agent-docs
        assertNotNull(jar.getJarEntry("agent-docs/README.md"));
    }
}
```

## Extension Points Documentation Pattern

For each extension interface, document:

```mermaid
graph LR
    subgraph "Extension Point Documentation"
        I[Interface Name] --> P[Purpose]
        P --> M[Methods Table]
        M --> D[Default Behavior]
        D --> E[Example Implementation]
        E --> R[Registration]
    end
```

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Diagram format | Mermaid | Better LLM comprehension, less tokens than ASCII |
| agent-docs location | Sibling to claude/ | Cleaner separation, both at resource root |
| Version substitution | Maven filtering | Built-in, no custom plugin needed |
| Test approach | JUnit with JarFile | Standard Java, runs in build |
| Reference module | dispatcher | Richest extension points |
