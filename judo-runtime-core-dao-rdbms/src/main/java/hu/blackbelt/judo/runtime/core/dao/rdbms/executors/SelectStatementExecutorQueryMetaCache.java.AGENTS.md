# `SelectStatementExecutorQueryMetaCache.java`

Precomputes, per `SubSelect` + mask + reference chain, every alias→`Node` and alias→`Target` map the select result mapper needs: `sources`, `types`, `versions`, `create*`/`update*` audit nodes, `metaFields`/`metaFieldNames`, `idFieldTargets`, `metaFieldTargets`, `featureTargetMappingMap`, single/multiple containment reference targets, `singleEmbeddedReferences`.
Built once per query in the constructor; maps are read-only afterwards — a query reshaped after construction needs a new cache.