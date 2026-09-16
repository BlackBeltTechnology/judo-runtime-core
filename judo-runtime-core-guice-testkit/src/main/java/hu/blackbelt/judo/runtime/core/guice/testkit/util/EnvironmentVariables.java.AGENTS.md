# `EnvironmentVariables.java`

Named set of environment variable values applied around a test body. Extends `SingularTestResource`, implements `NameValuePairSetter`; exports
`and(String,String)` (forks a new object), `set`/`remove` (mutate in place), `getVariables()`, and `doSetup`/`doTeardown`
which push and pop through `EnvironmentVariableMocker`. Varargs `(name, value, others...)` constructor requires
`others` to be even-numbered or throws `IllegalArgumentException`.