# `TestOperationCallInterceptorProvider.java`

Mutable `OperationCallInterceptorProvider` for tests. Exports `addInterceptor`, `removeInterceptor`, `clearInterceptors`,
`getInterceptors`, `size`, `isEmpty`, `getCallOperationInterceptors`. Interceptors fire in insertion order;
`addInterceptor(null)` throws `IllegalArgumentException`. Wire it through `JudoDefaultModule.builder().operationCallInterceptorProvider(...)`,
or prefer `JudoRuntimeFixture#addInterceptor`.