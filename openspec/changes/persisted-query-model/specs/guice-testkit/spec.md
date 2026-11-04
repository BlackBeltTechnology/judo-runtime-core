## ADDED Requirements

### Requirement: Fixture QueryFactory Through the Load-or-Build Decision
`JudoRuntimeFixture` SHALL obtain its `QueryFactory` through `QueryModelLoadOrBuild`, with a configurable query model mode. In mode `build` it SHALL build as before; in mode `load` it SHALL restore from the generated query artifact of its model and fail when it cannot.

#### Scenario: Build mode
- **GIVEN** a `JudoRuntimeFixture` with query model mode `build`
- **WHEN** the fixture is initialized
- **THEN** the `QueryFactory` is built from the model's JQL and the artifact is not opened

#### Scenario: Strict load mode
- **GIVEN** a `JudoRuntimeFixture` with query model mode `load` and a model whose generated files include a valid query artifact
- **WHEN** the fixture is initialized
- **THEN** the `QueryFactory` is restored and the testkit tests pass

#### Scenario: Strict load mode without the artifact
- **GIVEN** a `JudoRuntimeFixture` with query model mode `load` and a model without a query artifact
- **WHEN** the fixture is initialized
- **THEN** initialization fails with an error saying the artifact is missing
