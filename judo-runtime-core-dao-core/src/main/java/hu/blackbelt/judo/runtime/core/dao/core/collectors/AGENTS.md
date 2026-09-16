# AGENTS.md — `judo-runtime-core-dao-core/src/main/java/hu/blackbelt/judo/runtime/core/dao/core/collectors`

The instance-graph abstraction the DAO uses before mutating storage: one interface, its
no-op stand-in, and the two data carriers describing a collected graph. Persistence-specific
collectors (RDBMS, in-memory) live outside this package and implement `InstanceCollector`.

| File | Purpose |
| --- | --- |
| `EmptyMapIntstanceCollector.java` | No-op `InstanceCollector`: bulk `collectGraph` returns `ImmutableMap.of()`, single-id `collectGraph` returns `null`. Used where no storage graph exists; callers that dereference the single-id result get an NPE, so treat null as "no graph". Class name keeps the `Intstance` typo — do not silently rename, it is referenced by wiring. |
| `InstanceCollector.java` | Interface resolving persisted instances to their reachable graph. Declares `Map<Serializable, InstanceGraph> collectGraph(EClass, Collection<Serializable>)` and `InstanceGraph collectGraph(EClass, Serializable)`. Implementations decide whether an unknown id is absent from the map or mapped to null; callers must handle a missing key. |
| `InstanceGraph.java` | Node of a collected graph. Lombok `@Getter`/`@Builder` over `@NonNull Serializable id` plus three final `ArrayList`s: `containments`, `references`, `backReferences` of `InstanceReference`. The collections are final and never builder-settable — a collector must mutate them through the getter. `toString` omits empty collections. |
| `InstanceReference.java` | Directed edge in an `InstanceGraph`: `@NonNull EReference reference` plus `@NonNull final InstanceGraph referencedElement`, Lombok `@Getter`/`@Builder`. Both ends are required, so no dangling edge can be built; `toString` renders `referenceName:targetGraph` and recurses into the target. |
