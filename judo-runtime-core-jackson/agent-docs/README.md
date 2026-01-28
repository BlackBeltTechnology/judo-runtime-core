# JUDO Runtime Core :: Jackson ObjectMapper

Jackson JSON serialization configuration and custom type handlers for JUDO applications.

## Overview

This module provides a pre-configured Jackson `ObjectMapper` factory and custom serializers/deserializers for Java 8 date/time types. It ensures consistent JSON processing across all JUDO runtime components.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-jackson</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

**Packaging:** OSGi bundle

## Key Components

### ObjectMapperFactory
Factory for creating pre-configured ObjectMapper instances.

```java
public class ObjectMapperFactory {
    public static ObjectMapper createObjectMapper() {
        // Returns fully configured mapper
    }
}
```

**Registered Modules:**
- `ParameterNamesModule` - Constructor parameter names
- `Jdk8Module` - Optional and Stream support
- `JavaTimeModule` - Java 8 date/time support
- `JSR353Module` - JSON-P integration
- Custom serializer module for LocalDateTime

**Configuration:**
- `SerializationInclusion.NON_NULL` - Omit null values
- Auto-discovery of additional modules

### LocalDateTimeSerializer (`serializer/` package)
Custom serializer for `java.time.LocalDateTime` ensuring ISO-8601 format.

### LocalDateTimeDeserializer (`deserializer/` package)
Custom deserializer for `java.time.LocalDateTime` parsing ISO-8601 strings.

## Usage Example

```java
// Create configured ObjectMapper
ObjectMapper mapper = ObjectMapperFactory.createObjectMapper();

// Serialize object to JSON
String json = mapper.writeValueAsString(payload);

// Deserialize JSON to object
Map<String, Object> data = mapper.readValue(json, 
    new TypeReference<Map<String, Object>>() {});

// Handle LocalDateTime
LocalDateTime timestamp = LocalDateTime.now();
String timestampJson = mapper.writeValueAsString(timestamp);
// Result: "2024-01-15T10:30:00"
```

## JSON Format

The ObjectMapper produces JSON with these characteristics:

- **Null handling:** Null values are excluded from output
- **Dates:** ISO-8601 format (e.g., `"2024-01-15"`)
- **Times:** ISO-8601 format (e.g., `"10:30:00"`)
- **Timestamps:** ISO-8601 format (e.g., `"2024-01-15T10:30:00"`)
- **Optional:** Unwrapped values or null
- **Collections:** Standard JSON arrays

## Integration

This ObjectMapper configuration is used by:
- JAX-RS message body readers/writers
- REST API serialization
- Internal data transformation

## Dependencies

- `jackson-module-parameter-names` - Constructor parameter discovery
- `jackson-datatype-jdk8` - Java 8 types
- `jackson-datatype-jsr310` - Date/time types
- `jackson-datatype-jsr353` - JSON-P bridge

## Related Modules

- `judo-runtime-core-jaxrs` - JAX-RS providers using this ObjectMapper
- `judo-runtime-core-jaxrs-cxf` - CXF integration
