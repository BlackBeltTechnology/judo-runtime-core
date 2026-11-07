# AGENTS.md — `judo-runtime-core-dispatcher/src/main/java/hu/blackbelt/judo/runtime/core/dispatcher/converters`

Mapper `Formatter` implementations that convert dispatcher transfer types to and from transport
strings.

| File | Purpose |
| --- | --- |
| `FileTypeFormatter.java` | `Formatter<FileType>` round-tripping through Gson JSON. `convertValueToString(FileType)` folds id/fileName/size/mimeType into a `TreeMap` then serializes; `parseString(String)` rebuilds via `FileType.builder()`, casting `size` as `Double.valueOf(...).longValue()`. Exports `getType()` = `FileType.class`. Callers must only feed `parseString` the JSON this formatter emits. |