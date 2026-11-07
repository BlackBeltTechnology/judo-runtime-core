# AGENTS.md — `judo-runtime-core-dispatcher/src/main/java/hu/blackbelt/judo/runtime/core/dispatcher/context`

Per-thread `Context` implementation backing dispatcher request processing. Values live in a
static thread-local map; constructors rebuild that storage, so a new instance resets what any
thread sees.

| File | Purpose |
| --- | --- |
| `ThreadContext.java` | Implements `Context` over a static `THREADLOCAL`; constructors rebuild it (`inheritableContext=false` swaps to a plain `ThreadLocal`) and `putIfAbsent` returns `get(key)`, not the inserted value. → see `ThreadContext.java.AGENTS.md` |