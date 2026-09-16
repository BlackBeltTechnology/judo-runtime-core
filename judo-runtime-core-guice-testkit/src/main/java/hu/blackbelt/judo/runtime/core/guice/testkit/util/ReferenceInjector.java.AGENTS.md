# `ReferenceInjector.java`

Injects Guice bindings into objects OSGi would normally wire. Exports `injectReferences(Object, Injector)` and `createAndInject(Class<T>, Injector)`.
OSGi `@Reference` has CLASS retention and is invisible to reflection, so it instead fills every non-static non-final field
whose type the injector knows, plus `set*` single-argument setters. Null target or injector throws `IllegalArgumentException`;
a failing field or setter throws `IllegalStateException` naming the member and class.