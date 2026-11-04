# AGENTS.md — `hu/blackbelt/judo/runtime/core/jackson/serializer`

| File | Purpose |
| --- | --- |
| `LocalDateTimeSerializerTest.java` | JUnit 5 test for `LocalDateTimeSerializer`. Exports `testLocalDateTimeSerialization()`. Register serializer on `ObjectMapper` via `SimpleModule`; values serialize as UTC `ISO_OFFSET_DATE_TIME` strings (`atOffset(ZoneOffset.UTC)`), JSON key order asserted order-insensitive via `anyOf`. |