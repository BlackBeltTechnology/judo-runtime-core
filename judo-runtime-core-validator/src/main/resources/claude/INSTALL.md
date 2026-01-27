# Installing JUDO Validator Skills

## Module Information
- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-validator
- **Version**: ${project.version}

## Installation

### Extract from Maven cache

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-validator/${project.version}/judo-runtime-core-validator-${project.version}.jar
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Custom Validators | `/judo-validator:custom-validators` | Create custom validation logic |
| Validation Rules | `/judo-validator:validation-rules` | Configure and debug validation rules |

## Key Extension Points

- `Validator` - Custom validation logic interface
- `ValidatorProvider` - Validator lifecycle management
- `DefaultPayloadValidator` - Main validation orchestrator
