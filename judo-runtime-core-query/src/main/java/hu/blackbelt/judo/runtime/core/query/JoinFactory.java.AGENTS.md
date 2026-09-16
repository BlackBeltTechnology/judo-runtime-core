# `JoinFactory.java`

Abstract factory unfolding expression navigation chains (references, filters, order, cast, container, object-selector, limit/offset)
into `Join`/`Filter`/`OrderBy`/`SubSelect` nodes. Exports `convertNavigationToJoins(Context, Node, ReferenceExpression, boolean)`
returning `PathEnds` (base, partner, limit, offset, ordered); subclasses implement protected abstract `expressionToFeature(...)`.
Throws `NoSuchElementException` for unknown reference names and `IllegalStateException` for invalid variable-reference types;
`checkArgument` rejects derived references and collection navigation inside limited contexts. Join aliases consume `context.getSourceCounter()`.