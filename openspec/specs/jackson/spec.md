# Jackson Serialization Specification

## Purpose
Provides a pre-configured Jackson `ObjectMapper` factory with custom `LocalDateTime` serialization and deserialization support, ensuring consistent JSON date/time handling across the JUDO runtime using ISO 8601 offset format with UTC normalization.

## Architecture
- **`ObjectMapperFactory`** -- Static factory that creates and configures a shared `ObjectMapper` instance with multiple Jackson modules and a custom `LocalDateTime` serializer/deserializer pair.
- **`LocalDateTimeSerializer`** -- Serializes `LocalDateTime` values to ISO 8601 offset date-time strings with explicit UTC offset (`+00:00`).
- **`LocalDateTimeDeserializer`** -- Deserializes JSON strings to `LocalDateTime`, supporting both ISO local date-time and ISO offset date-time input formats.

### Class Relationships
```
ObjectMapperFactory
  ├── registers LocalDateTimeSerializer
  ├── registers LocalDateTimeDeserializer
  ├── registers ParameterNamesModule
  ├── registers Jdk8Module
  ├── registers JavaTimeModule
  └── registers JSR353Module
```

### Package
`hu.blackbelt.judo.runtime.core.jackson`

## Requirements

### Requirement: ObjectMapper creation with standard module registration
The `ObjectMapperFactory.createObjectMapper()` method SHALL return an `ObjectMapper` configured with `ParameterNamesModule`, `Jdk8Module`, `JavaTimeModule`, `JSR353Module`, and a custom serializer module for `LocalDateTime`.

#### Scenario: Creating a default ObjectMapper
- **GIVEN** no prior ObjectMapper exists
- **WHEN** `ObjectMapperFactory.createObjectMapper()` is invoked
- **THEN** the returned `ObjectMapper` has `ParameterNamesModule`, `Jdk8Module`, `JavaTimeModule`, `JSR353Module`, and the custom `LocalDateTime` module registered

### Requirement: Null exclusion in serialization
The `ObjectMapper` created by `ObjectMapperFactory` SHALL exclude null values from serialized JSON output by using `JsonInclude.Include.NON_NULL`.

#### Scenario: Serializing an object with null fields
- **GIVEN** an `ObjectMapper` created by `ObjectMapperFactory.createObjectMapper()`
- **WHEN** an object with some null-valued fields is serialized to JSON
- **THEN** the resulting JSON string does not contain keys for the null-valued fields

### Requirement: Auto-discovery of additional Jackson modules
The `ObjectMapper` SHALL call `findAndRegisterModules()` to automatically discover and register any Jackson modules available on the classpath.

#### Scenario: Classpath contains additional Jackson modules
- **GIVEN** an additional Jackson module JAR is present on the classpath
- **WHEN** `ObjectMapperFactory.createObjectMapper()` is invoked
- **THEN** the additional module is discovered and registered on the returned `ObjectMapper`

### Requirement: LocalDateTime serialization to ISO offset format
`LocalDateTimeSerializer` SHALL serialize a `LocalDateTime` value as an ISO 8601 offset date-time string with UTC offset using `DateTimeFormatter.ISO_OFFSET_DATE_TIME`.

#### Scenario: Serializing a LocalDateTime value
- **GIVEN** a `LocalDateTime` value of `2024-03-15T10:30:00`
- **WHEN** the value is serialized by `LocalDateTimeSerializer.serialize()`
- **THEN** the output JSON string is `"2024-03-15T10:30:00Z"` (or equivalent `+00:00` suffix)

### Requirement: LocalDateTime deserialization from ISO local format
`LocalDateTimeDeserializer` SHALL deserialize an ISO local date-time string (without offset) directly to a `LocalDateTime` using `DateTimeFormatter.ISO_LOCAL_DATE_TIME`.

#### Scenario: Deserializing a local date-time string
- **GIVEN** a JSON string value `"2024-03-15T10:30:00"`
- **WHEN** the value is deserialized by `LocalDateTimeDeserializer.deserialize()`
- **THEN** the result is `LocalDateTime.of(2024, 3, 15, 10, 30, 0)`

### Requirement: LocalDateTime deserialization from ISO offset format with UTC conversion
`LocalDateTimeDeserializer` SHALL fall back to parsing as `ISO_OFFSET_DATE_TIME` when `ISO_LOCAL_DATE_TIME` parsing fails, and SHALL convert the result to UTC before returning a `LocalDateTime`.

#### Scenario: Deserializing an offset date-time string
- **GIVEN** a JSON string value `"2024-03-15T12:30:00+02:00"`
- **WHEN** the value is deserialized by `LocalDateTimeDeserializer.deserialize()`
- **THEN** the result is `LocalDateTime.of(2024, 3, 15, 10, 30, 0)` (converted to UTC)
