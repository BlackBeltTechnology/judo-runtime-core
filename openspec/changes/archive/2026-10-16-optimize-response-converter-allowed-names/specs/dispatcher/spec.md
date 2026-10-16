## MODIFIED Requirements

### Requirement: Response Payload Conversion
`ResponseConverter` SHALL convert outbound payloads by mapping enumeration ordinals to literals, coercing attribute values, and converting binary data to download tokens. It SHALL also filter every response row, including every nested embedded row, down to the contract of that row's transfer object type. A key SHALL be kept if and only if both of these hold:
- the key is a public member of the row's type, or a configured keep property (metadata key)
- the key's value is non-null

A public member is any structural feature of the type (including inherited ones) that is not the target of another feature's `default` annotation on the same type. Those targets are the hidden default-value companions.

#### Scenario: Enumeration ordinal to literal
- **GIVEN** a DAO result with an integer value for an `EEnum`-typed attribute
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** the integer is mapped to the corresponding enumeration literal string

#### Scenario: Binary attribute to download token
- **GIVEN** a DAO result containing a `FileType` for a byte-array attribute
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** the `FileType` is converted to a download token string via `TokenIssuer.createDownloadToken`

#### Scenario: Declared feature is kept
- **GIVEN** a response row whose key is a public structural feature of the transfer object type and whose value is non-null
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** the key and its value are present in the converted payload

#### Scenario: Unknown key is removed
- **GIVEN** a response row containing a key that is neither a structural feature of the type nor a keep property
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** the key is absent from the converted payload

#### Scenario: Null value is removed
- **GIVEN** a response row with a declared feature or a keep property whose value is `null`
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** the key is absent from the converted payload

#### Scenario: Keep property is kept although not a feature
- **GIVEN** a converter configured with keep properties (e.g. the identifier, `__updateable`, `__deleteable`, `__entityType`, `__version`)
- **AND** a response row containing such a key with a non-null value
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** the key and its value are present in the converted payload

#### Scenario: Default-value companion is hidden
- **GIVEN** a transfer object type where feature `name` carries a `default` annotation naming feature `_name_default`
- **AND** a response row containing both `name` and `_name_default`
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** `name` is kept and `_name_default` is absent from the converted payload

#### Scenario: Nested embedded row uses its own type's rules
- **GIVEN** a transfer object type with an embedded reference to another transfer object type
- **AND** a response row whose nested payload contains a feature of the nested type, a default-value companion of the nested type, a feature belonging only to the outer type, and an unknown key
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** only the nested type's public feature remains in the nested payload

#### Scenario: Repeated conversions apply identical rules
- **GIVEN** a single `ResponseConverter` instance
- **WHEN** it converts several payloads in succession
- **THEN** every conversion keeps and removes exactly the same keys as a conversion run in isolation

## ADDED Requirements

### Requirement: Response Key Filtering Cost Independent of Type Width Per Key
The decision whether a response key is a public member of its row's type SHALL be derived from the type's structural features at most once per distinct type per conversion. Checking an individual key against that derived result SHALL take constant time. It SHALL NOT rescan the type's structural features or evaluate model annotations for each key or each row.

#### Scenario: Wide transfer object list conversion
- **GIVEN** a list response of 202 rows whose transfer object type declares 86 members
- **WHEN** `ResponseConverter.convert` processes the response
- **THEN** the work spent deriving public member names grows with the number of distinct types in the response, not with rows × keys × members
- **AND** the converted payload is identical to the one produced by the previous per-key evaluation

#### Scenario: No state retained across conversions
- **GIVEN** a `ResponseConverter` that has completed a conversion
- **WHEN** a subsequent conversion is run, possibly against a reloaded model
- **THEN** the public-member decision is derived again from the current type and nothing derived in an earlier conversion is reused
