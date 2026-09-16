# AGENTS.md — `judo-runtime-core-dao-core/src/main/java/hu/blackbelt/judo/runtime/core/dao/core/values`

Value carriers the statements in `..core.statements` operate on: the instance being
written, its attribute slots, its reference slots, and the audit trio.

| File | Purpose |
| --- | --- |
| `AttributeValue.java` | Pairs one `EAttribute` with its typed value `O`. Built through `attributeValueBuilder()`; `attribute` is `@NonNull` while `value` may be null, so a null here means "set to null", not "absent". `toString` renders `name=value` using the plain attribute name, not an FQ name. |
| `InstanceValue.java` | Statement target: `@NonNull EClass type` plus `@NonNull Serializable identifier` and a `@Builder.Default` mutable `List<AttributeValue<Object>> attributes`. Exports `buildInstanceValue()` and `addAttributeValue(EAttribute, Object)`, which appends without de-duplicating. `@EqualsAndHashCode` covers the attribute list, so equality shifts as attributes are added. |
| `Metadata.java` | Audit triple `userId` / `username` / `timestamp` (`LocalDateTime`) built by `buildMetadata()`. All three fields are optional and unvalidated; consumers must tolerate a fully empty `Metadata`. |
| `ReferenceValue.java` | Describes one reference slot: `@NonNull` `type`, `identifier`, `reference`, plus `oppositeIdentifiers` defaulting to an immutable empty `ImmutableList`. Built by `referenceValueBuilder()`; the default collection is immutable, so callers must supply a fresh collection rather than mutating the default. |
