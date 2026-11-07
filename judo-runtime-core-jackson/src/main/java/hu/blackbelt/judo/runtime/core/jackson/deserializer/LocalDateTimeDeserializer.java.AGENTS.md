# `LocalDateTimeDeserializer.java`

`JsonDeserializer<LocalDateTime>` parsing timestamp strings in
`deserialize(JsonParser, DeserializationContext)`. First tries `ISO_LOCAL_DATE_TIME`;
on `DateTimeParseException` falls back to `ISO_OFFSET_DATE_TIME`, converting via
`atZoneSameInstant(ZoneOffset.UTC)` — so offset-bearing inputs normalize to UTC and lose
their zone. Callers sending an offset timestamp expect the wall-clock offset preserved
and must know it is shifted to UTC.