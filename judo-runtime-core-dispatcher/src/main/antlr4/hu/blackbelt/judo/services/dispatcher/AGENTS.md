# AGENTS.md — `judo-runtime-core-dispatcher/src/main/antlr4/hu/blackbelt/judo/services/dispatcher`

ANTLR4 grammar sources for the dispatcher module — parse recognized into listeners/visitors by generated parser code.

| File | Purpose |
| --- | --- |
| `QueryMask.g4` | Defines ANTLR4 grammar `QueryMask` for query masks. Entry rule `parse` matches `{ memberList? }` then EOF; `memberList` = comma-separated `member`s; `member` = `attribute` (`IDENTIFIER`) or `relation` (`IDENTIFIER` plus `{ memberList? }`), so masks nest relation member lists. Contract: a relation member always carries braces; a bare identifier without braces parses as attribute. |