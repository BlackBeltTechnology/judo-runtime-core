# `SimpleLiquibaseExecutor.java`

Applies a Liquibase changelog against a `DataSource`.
`executeInitiLiquibase(ClassLoader, String, DataSource)` runs a classpath-resource changelog via `liquibase.update(new Contexts(), new LabelExpression())`; `createDatabase`/`dropDatabase` run `update((String) null)` / `dropAll()` through `executueOnLiquibaseModel(DataSource, LiquibaseModel, Consumer<Liquibase>)`, which saves the model to a stream, feeds it through `CompositeResourceAccessor` of `StreamResourceAccessor` + classloader, and skips empty or invalid models.