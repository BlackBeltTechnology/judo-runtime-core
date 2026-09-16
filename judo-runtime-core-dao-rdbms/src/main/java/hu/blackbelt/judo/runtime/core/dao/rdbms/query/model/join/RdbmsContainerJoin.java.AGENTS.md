# `RdbmsContainerJoin.java`

Joins back to the container of a composition, following several `EReference`s at once. `@SuperBuilder` over `@NonNull tableName` and `@NonNull List<EReference> references`; exports `POSTFIX = "_c"`.
Builds one partner alias per reference index (`<prefix><alias>_c<i>`) and, for more than one, matches with `COALESCE(a_c0.col, a_c1.col, …) = <alias>.<columnName>`.
`checkArgument` fails when the reference list yields no partner.