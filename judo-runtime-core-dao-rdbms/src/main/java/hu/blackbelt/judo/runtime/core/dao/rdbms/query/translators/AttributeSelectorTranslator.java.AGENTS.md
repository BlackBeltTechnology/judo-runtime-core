# `AttributeSelectorTranslator.java`

Rewrites an `AttributeSelector` node from transfer-object grounding to entity grounding,
recursing into its object expression through the injected dispatcher. This is the one
translator whose output changes names, not just structure: it maps the selector's TO
attribute onto the entity attribute name and, for derived attributes, substitutes the
entity-side getter expression.

**Key exports**

- `@Builder` class `AttributeSelectorTranslator implements Function<AttributeSelector, Expression>`.
- `apply(AttributeSelector)` → `Expression`: the rewritten selector, or the substituted getter
  `DataExpression` for derived attributes.
- Builder deps (`@NonNull`): `translator` (`Function<Expression, Expression>`), `asmModelAdapter`
  (`AsmModelAdapter`), `queryFactory` (`QueryFactory`), `asmUtils` (`AsmUtils`).

**Behaviour**

- Resolves the transfer attribute by name on the mapped TO type
  (`attributeSelector.getObjectExpression().getObjectType(asmModelAdapter)`); an attribute with
  no such name throws `IllegalStateException("Attribute not found: …")`.
- Maps it via `asmUtils.getMappedAttribute(transferAttribute)`; an unmapped attribute throws
  `IllegalStateException("Attribute is not mapped: …")`.
- Non-derived: copies the selector (`EcoreUtil.copy`), rewrites the object expression through
  `translator`, renames the attribute to the entity attribute name.
- Derived: takes the getter `DataExpression` from
  `queryFactory.getEntityTypeExpressionsMap().get(container).getGetterAttributeExpressions()`,
  clones it, rewrites the object expression, then rebinds every self-reference
  (`JqlExpressionBuilder.SELF_NAME` naming an `Instance` variable) to the resolved object
  variable.

**Contracts a caller can violate**

- The mapped TO type must declare the selector's attribute name and that attribute must carry
  an entity mapping — both violations fail at translation time with `IllegalStateException`.
- The base object expression must resolve to an `ObjectVariableReference` or `ObjectVariable`;
  any other shape throws `IllegalStateException("Unsupported object variable")`.
- Only variables named `JqlExpressionBuilder.SELF_NAME` bound to an `Instance` count as
  self-references; other references keep their own variable binding.