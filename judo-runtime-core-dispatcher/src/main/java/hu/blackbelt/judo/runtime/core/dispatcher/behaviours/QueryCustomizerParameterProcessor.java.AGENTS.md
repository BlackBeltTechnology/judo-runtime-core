# `QueryCustomizerParameterProcessor.java`

Builds `DAO.QueryCustomizer` from query-customizer-shaped maps: filter per attribute, `_orderBy`, `_seek`, `_mask` via `QueryMaskStringParser.parseQueryMask`, `_identifier` coerced to the `IdentifierProvider` type.
Static `convertFilterToJql(EAttribute, operator, value, caseInsensitiveLike)` emits `this.<attr>` JQL with `!isUndefined()`/`!isDefined()` for null `EQ`/`NEQ`, measure/unit suffixes, `!matches`, `!ilike` when case-insensitive, and enumeration `FQN#member`.
Contract: unsupported operators/values throw `IllegalArgumentException`; bad mask/identifier raise `ValidationException` with codes `INVALID_QUERY_MASK`/`INVALID_IDENTIFIER_IN_QUERY_CUSTOMIZER`.