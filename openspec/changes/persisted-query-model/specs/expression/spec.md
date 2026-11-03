## REMOVED Requirements

### Requirement: Mapped transfer object graph resolution
**Reason**: `judo-runtime-core-expression` is removed; `TransferObjectTypeBindingsCollector` moves to
`judo-tatami-base/judo-tatami-asm2query`, where it gains a one-pass binding index with identical output.
**Migration**: Use `hu.blackbelt.judo.tatami.asm2query.TransferObjectTypeBindingsCollector` (capability `asm2query`
in `judo-tatami-base`); depend on `judo-tatami-asm2query` instead of `judo-runtime-core-expression`.

### Requirement: Unmapped transfer object bindings resolution
**Reason**: Moved to tatami-base `asm2query` together with the collector.
**Migration**: Use `getTransferObjectBindings()` of the moved collector in package
`hu.blackbelt.judo.tatami.asm2query`.

### Requirement: Attribute binding role classification
**Reason**: Moved to tatami-base `asm2query` together with the collector.
**Migration**: The same role classification lives in the moved collector in package
`hu.blackbelt.judo.tatami.asm2query`.

### Requirement: Reference binding role classification
**Reason**: Moved to tatami-base `asm2query` together with the collector.
**Migration**: The same role classification lives in the moved collector in package
`hu.blackbelt.judo.tatami.asm2query`.

### Requirement: Filter binding resolution
**Reason**: Moved to tatami-base `asm2query` together with the collector.
**Migration**: The same filter resolution lives in the moved collector in package
`hu.blackbelt.judo.tatami.asm2query`.

### Requirement: Static expression detection
**Reason**: Moved to tatami-base `asm2query`. At runtime, a restored `QueryFactory` answers `isStaticAttribute` and
`isStaticReference` from the persisted static sets instead of a collector.
**Migration**: Build-time callers use the moved collector in `hu.blackbelt.judo.tatami.asm2query`; runtime callers
keep calling `QueryFactory.isStaticAttribute` / `isStaticReference`.

### Requirement: Entity type expressions map
**Reason**: Moved to tatami-base `asm2query` (`EntityTypeExpressions` changes package); a restored `QueryFactory`
serves the map from the persisted `entityTypeExpressions` entries.
**Migration**: Import `EntityTypeExpressions` from `hu.blackbelt.judo.tatami.asm2query`; callers of
`QueryFactory.getEntityTypeExpressionsMap()` recompile against the new type.
