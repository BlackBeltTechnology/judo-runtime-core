# `LocalDateTimeSerializer.java`

`JsonSerializer<LocalDateTime>` emitting timestamps in `serialize(...)`: formats the
value via `DateTimeFormatter.ISO_OFFSET_DATE_TIME` after `atOffset(ZoneOffset.UTC)`, so
output always carries the literal `Z`/`+00:00` offset suffix and a UTC-normalized wall
clock. A value with a non-UTC local time round-trips shifted (its original local
wall-clock is lost); the paired `LocalDateTimeDeserializer` re-applies UTC on parse,
keeping serialization symmetric.