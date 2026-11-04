# `Context.java`

`@Builder @Getter` mutable state threaded through query construction: `node` (settable), `createdQueryObjects`, `TreeMap`
`variables`, `AtomicInteger` `sourceCounter`/`targetCounter`. Exports `addFeature(Feature)`, `clone()`, `clone(String, Node)`.
`addFeature` routes to `node.getFeatures()`, the containing node for `OrderBy`, or join base for `Join` and throws
`IllegalStateException` for other node kinds; callers must set `node` first (else `checkArgument` fails).
`clone(String, Node)` registers one variable mapping in a fresh variable map.