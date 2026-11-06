# `JudoTest.java`

Meta-annotation that combines `@Test` with `@ExtendWith(JudoTestExtension.class)`, so a
single `@JudoTest` on a method or class both registers the test and stands up a JUDO
runtime. `@Target({METHOD, TYPE, ANNOTATION_TYPE})`, `@Retention(RUNTIME)` — it is usable
as a class-level default and as a meta-annotation on a custom composed annotation.

## Attributes

| Attribute | Default | Meaning |
| --- | --- | --- |
| `modelName()` | `"example"` | JUDO model to load. |
| `dialect()` | `"hsqldb"` | `hsqldb` or `postgresql`. Env var `JUDO_TEST_DIALECT` wins over this value. |
| `container()` | `"none"` | `none`, `postgresql` or `yugabytedb`. |
| `transaction()` | `TransactionHandling.AUTO_ROLLBACK` | How the per-test transaction is settled. |
| `truncateTables()` | `true` | Truncate tables after the test (applies to `AUTO_COMMIT` and `MANUAL`). |
| `modelSource()` | `ModelSource.AUTO` | Where the model is read from. |
| `dataSourceMode()` | `DataSourceMode.BY_METHOD` | Lifecycle scope of the datasource. |
| `shareInjector()` | `false` | Allow runtime/injector reuse in class-scoped modes. |

## Nested enums

- `TransactionHandling` — `AUTO_ROLLBACK`, `AUTO_COMMIT`, `MANUAL`, `NONE`.
- `ModelSource` — `AUTO`, `FILESYSTEM`, `CLASSPATH`.
- `DataSourceMode` — `BY_METHOD`, `BY_CLASS`, `SINGLETON`.

## Contracts a caller can violate

- Configuration priority is env var > annotation > default. A test that asserts on
  `dialect()` directly, rather than the resolved dialect, breaks under
  `JUDO_TEST_DIALECT`.
- `shareInjector()` only has an effect for `BY_CLASS` and `SINGLETON`; on `BY_METHOD`, or on
  a method-level annotation, it is ignored (see `JudoTestExtensionRouting#useCache`).
- `SINGLETON` keeps the datasource alive for the whole JVM — cross-class isolation is the
  test author's problem, not the extension's.
