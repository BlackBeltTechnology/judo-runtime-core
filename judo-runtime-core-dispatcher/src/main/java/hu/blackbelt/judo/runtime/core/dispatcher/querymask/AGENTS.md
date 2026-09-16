# AGENTS.md — `judo-runtime-core-dispatcher/src/main/java/hu/blackbelt/judo/runtime/core/dispatcher/querymask`

Turns a textual query-mask expression (`name,address(city,zip)`) into the nested
`Map<String, Object>` mask the DAO consumes, using the generated ANTLR `QueryMaskLexer` /
`QueryMaskParser` from `hu.blackbelt.judo.services.dispatcher`.

| File | Purpose |
| --- | --- |
| `QueryMaskParserErrorListener.java` | `ANTLRErrorListener` that turns every parser callback into a failure. Exports `isFail()`, `getMessage()`, `setFail(boolean,String)`; constructor takes the parsed sentence, which is embedded in the message. Treats ambiguity, full-context attempts and context sensitivity as errors too, not just `syntaxError`, so a "successful" parse is only valid when `isFail()` is false. |
| `QueryMaskParserListener.java` | `QueryMaskBaseListener` that builds the nested mask map while walking the parse tree, validating every identifier against the current `EClass`. → see `QueryMaskParserListener.java.AGENTS.md` |
| `QueryMaskStringParser.java` | Abstract entry point exporting static `parseQueryMask(EClass, String)`. Wires `QueryMaskLexer` → `CommonTokenStream` → `QueryMaskParser`, attaches `QueryMaskParserErrorListener`, walks `parse()` with `QueryMaskParserListener` and returns its result map. `clazz` must be non-null; a `null` mask string returns `null` (no mask), while a parse error throws `IllegalArgumentException`. |
