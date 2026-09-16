# AGENTS.md — `hu/blackbelt/judo/runtime/core/jackson/deserializer`

| File | Purpose |
| --- | --- |
| `LocalDateTimeDeserializerTest.java` | JUnit 5 test for `LocalDateTimeDeserializer`. Exports `testLocalDateTimeDeserialization()`. Register deserializer on `ObjectMapper` via `SimpleModule`; local strings parse with `ISO_LOCAL_DATE_TIME`, offset strings convert through UTC (`atZoneSameInstant(ZoneOffset.UTC)`), invalid input throws `DateTimeParseException`. |