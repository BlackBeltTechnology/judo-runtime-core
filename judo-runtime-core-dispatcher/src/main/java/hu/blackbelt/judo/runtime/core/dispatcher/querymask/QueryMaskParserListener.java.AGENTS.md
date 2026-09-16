# AGENTS.md — `QueryMaskParserListener.java`

`QueryMaskBaseListener` subclass that accumulates the nested mask map while ANTLR walks a
parsed query-mask sentence.

Exports: constructor `QueryMaskParserListener(EClass)`, `getResult()`, overrides
`enterAttribute`, `enterRelation`, `exitRelation`, `exitParse`; constants `TAB`, `SEP`.

State and contracts:

- Holds a `Deque<Context>` of scopes; each private `Context` pairs the current `EClass`, the
  relation name that opened it, and a `TreeMap` mask, so key order is alphabetical.
- `enterAttribute` validates the identifier against `clazz.getEAllAttributes()` and puts
  `name -> true`; an unknown attribute fails `checkArgument` with
  `Attribute: %s not found on %s`.
- `enterRelation` validates against `clazz.getEAllReferences()`, pushes the current context and
  descends into `EReference.getEReferenceType()`; unknown relation fails `checkArgument`
  the same way.
- `exitRelation` pops the parent and nests the child map under the relation name, so the result
  is a tree of `Map<String, Object>`.
- `getResult()` returns `null` until `exitParse` assigns the root context mask — calling it on a
  partially walked tree is a caller error.
- `indent` only drives `log.trace` output; it carries no semantics.
