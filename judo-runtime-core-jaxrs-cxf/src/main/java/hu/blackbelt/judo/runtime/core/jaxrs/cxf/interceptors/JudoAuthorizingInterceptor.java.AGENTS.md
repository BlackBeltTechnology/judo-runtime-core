# `JudoAuthorizingInterceptor.java`

`AbstractAuthorizingInInterceptor` deriving CXF expected roles from the ASM
model. Lombok `@Builder` on `(AsmModel asmModel)`; exports
`getExpectedRoles(Method)`.

Resolves the `@JudoOperation` value to an `EOperation`, walks its `exposedBy`
extension-annotation access points, and returns empty roles when any exposing
actor is public (missing or empty `realm`). Otherwise returns the access-point
FQ names via `AsmUtils.getClassifierFQName`.

Throws `IllegalStateException` when the operation or an access point is absent
from the ASM model. Methods without `@JudoOperation` require no roles and yield
an empty list.
