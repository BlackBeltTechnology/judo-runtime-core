# `DefaultPayloadValidatorProvider.java`

Guice `Provider<PayloadValidator>`. `get()` builds `DefaultPayloadValidator` from
`JudoModelLoader`'s `AsmModel`, coercer from `DataTypeManager`, `IdentifierProvider`,
`ValidatorProvider`. Declares `ACCEPT_NON_EMPTY` constant; optional
`@PayloadValidatorRequiredStringValidatorOption` string parsed via
`RequiredStringValidatorOption.valueOf`, defaulting to `ACCEPT_NON_EMPTY` — invalid bound
value throws `IllegalArgumentException`.