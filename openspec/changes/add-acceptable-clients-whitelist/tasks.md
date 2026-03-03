## 1. Configuration Parsing

- [x] 1.1 Add `acceptableClients` string parameter to `DefaultActorResolver.Builder` (nullable, defaults to null)
- [x] 1.2 Implement parsing logic: split by `;` for entries, split by `=` for key-value, split by `,` for client list → `Map<String, Set<String>>`
- [x] 1.3 Validate no duplicate client IDs across actor type entries at construction time; throw `IllegalArgumentException` if ambiguous
- [x] 1.4 Store parsed map and log parsed configuration at INFO level on startup

## 2. Core Resolution Logic

- [x] 2.1 Modify `authenticateByPrincipal` to extract actor type resolution into a helper: try `asmUtils.resolve(principal.getClient())` first
- [x] 2.2 Add fallback: if resolve returns empty, search acceptable clients map values for `principal.getClient()`, use matched key as actor FQN
- [x] 2.3 Resolve the matched actor FQN via `asmUtils.resolve()` and proceed with existing flow
- [x] 2.4 Preserve existing "Unsupported client" exception when neither direct resolution nor map lookup succeeds

## 3. Guice Integration

- [x] 3.1 Add `acceptableClients` string binding in the Guice module that constructs `DefaultActorResolver`
- [x] 3.2 Pass the string to `DefaultActorResolver.Builder`

## 4. Spring Integration

- [x] 4.1 Add `acceptableClients` property to Spring autoconfiguration for `DefaultActorResolver`
- [x] 4.2 Pass the property to `DefaultActorResolver.Builder`

## 5. Platform Integration (judo-platform)

- [x] 5.1 Add `acceptableClients` as `@AttributeDefinition` in the appropriate activator's `Config` interface
- [x] 5.2 Add `acceptableClients` to the PIDS map entry for the component that constructs `DefaultActorResolver`
- [x] 5.3 Verify `JUDO_PLATFORM_ACCEPTABLE_CLIENTS` environment variable is picked up via the osgi-configuration-mapper

## 6. Tests

- [x] 6.1 Unit test: parsing valid config string into correct map structure
- [x] 6.2 Unit test: empty/null config results in empty map and unchanged behavior
- [x] 6.3 Unit test: ambiguous mapping (same client in multiple actors) throws `IllegalArgumentException`
- [x] 6.4 Unit test: `authenticateByPrincipal` resolves actor FQN client directly (existing behavior)
- [x] 6.5 Unit test: `authenticateByPrincipal` resolves whitelisted non-actor client via map fallback
- [x] 6.6 Unit test: `authenticateByPrincipal` throws for unknown client not in map
