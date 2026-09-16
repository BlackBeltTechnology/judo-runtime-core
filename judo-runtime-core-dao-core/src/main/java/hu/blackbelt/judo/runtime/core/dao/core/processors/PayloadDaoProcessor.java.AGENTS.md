# `PayloadDaoProcessor.java`

Base of every processor in this package (340 lines). Holds the collaborators, the reusable
`EStructuralFeature` predicates, the reserved payload keys, and the structural validations that
insert/update/delete all call before emitting statements.

## Collaborators and constants

- `@NonNull` fields: `resourceSet` (getter only; `setResourceSet` also rebuilds `asmUtils`),
  `identifierProvider`, `queryFactory`, `instanceCollector` — the last three have setters.
- `asmUtils` is derived from the `ResourceSet`, exposed via `getAsmUtils()`.
- `MEASURE_CONVERTING_SCALE = 20` — scale used when coercing measured attribute values.
- Reserved payload keys: `REFERENCE_ID = "__referenceId"`, `ENTITY_TYPE_KEY = "__entityType"`,
  `VERSION = "__version"`. Payload authors must not use these names for model features.

## Predicate vocabulary (public static)

Feature shape: `isSingle`, `isCollection`, `isMandatory`, `isDerived`, `notDerived`,
`isChangeable`, `notChangeable`; reference shape: `isContainment`, `hasOpposite`.

Payload-relative factories: `hasPayload`, `hasNotPayload`, `hasPayloadNotNull`,
`hasNotPayloadOrNull`, `payloadTypeIsInstanceOf`, `payloadTypeIsNotInstanceOf`,
`notParent(EReference)`, `hasReferenced(payload, identifierName)` (payload present **with** an id),
`hasEmbedded(payload, identifierName)` (payload present **without** an id). The referenced/embedded
split is the rule the update processor branches on — do not conflate them.

`toReferencePayloadMapOfPayloadCollection` is a `Collector` normalising single and many references
into `Map<EReference, Collection<Payload>>`.

## Validations (all throw `IllegalArgumentException` via `checkArgument`)

- `checkMandatoryReferences` / `checkMandatoryAttributes` — a required feature absent or null fails.
- `checkReferences` — single reference payload must be a `Map`, many must be a `Collection`.
- `checkForbiddenReferenceUpdates` — a reference with an opposite whose `lowerBound > 0` cannot be
  re-pointed by id, because the implicit detach would violate the opposite's constraint.
- `checkAssociationCannotBeEmbedded` — a non-containment reference must carry an id, not an
  embedded payload.
- `checkMappedObjectStructure(EClass, EReference)` — `checkState`s that the transfer-object type has
  a mapped entity, then resolves the entity's `defaultRepresentation` and its `default`/`binding`
  annotated attributes.

## Other surface

- `getModelAdapter()` delegates to `queryFactory.getModelAdapter()`.
- `getTransferObjectValueAsEntityValueFromPayload(Payload, EAttribute transfer, EAttribute entity)`
  converts a transfer attribute value into the entity's `Unit`, rounding at
  `MEASURE_CONVERTING_SCALE` with `RoundingMode`.

## Contracts

- The `@NoArgsConstructor` exists for Lombok/proxy use; instances built that way have null
  collaborators and will NPE — subclasses use the four-arg `@NonNull` constructor.
- Predicates are public static mutable fields, not constants — reassigning one changes behaviour
  process-wide.
