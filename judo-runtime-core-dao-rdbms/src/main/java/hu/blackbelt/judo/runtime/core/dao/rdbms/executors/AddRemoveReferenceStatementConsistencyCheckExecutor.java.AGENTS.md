# `AddRemoveReferenceStatementConsistencyCheckExecutor.java`

Pre-flight crosscheck; no SQL mutation.
`checkRemoveReferenceStatements` and `checkAddReferenceStatements` reject statement batches that would leave a mandatory reference unset or break a back reference: `"There is reference remove which let referrer violate mandatory constraint"`, `"...reference add which let referrer violate mandatory constraint"`, `"...let back reference violate constraint"`.
Cardinality checks exist but are commented out.