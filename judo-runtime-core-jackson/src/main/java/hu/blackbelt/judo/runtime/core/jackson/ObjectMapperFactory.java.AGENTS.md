# `ObjectMapperFactory.java`

Builds the shared Jackson `ObjectMapper` via `createObjectMapper()`. Registers
`ParameterNamesModule`, `Jdk8Module`, `JavaTimeModule`, `JSR353Module`, and a
`SimpleModule` wiring `LocalDateTimeSerializer`/`LocalDateTimeDeserializer` for
`LocalDateTime`. Calls `findAndRegisterModules()` and sets
`JsonInclude.Include.NON_NULL`. Callers keying on default Jackson `LocalDateTime`
handling break, since the custom pair overrides it.