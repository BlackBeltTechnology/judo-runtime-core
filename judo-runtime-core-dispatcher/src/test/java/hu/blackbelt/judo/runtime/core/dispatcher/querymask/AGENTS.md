# AGENTS.md — `querymask`

| File | Purpose |
| --- | --- |
| `QueryMaskStringParserTest.java` | Tests `QueryMaskStringParser.parseQueryMask(EClass, String)` on EMF `EClass` trees built with `EcoreBuilders`. Asserts null mask → null, `{}` → empty map, attribute entries → `true`, nested reference masks → nested `Map<String,Object>`. Callers re-running tests must keep asserted mask shapes in sync with parser output. |