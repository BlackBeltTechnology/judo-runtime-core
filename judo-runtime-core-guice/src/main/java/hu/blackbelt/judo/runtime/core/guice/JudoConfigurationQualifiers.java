package hu.blackbelt.judo.runtime.core.guice;

import com.google.inject.BindingAnnotation;

import javax.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class JudoConfigurationQualifiers {
    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface QueryFactoryCustomJoinDefinitions {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface RdbmsDaoOptimisticLockEnabled {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface RdbmsDaoChunkSize {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface RdbmsDaoMaximumRecursionCount {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface ActorResolverCheckMappedActors {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface ActorResolverAcceptableClients {}

    // JNG-6415 locale qualifiers (ActorResolverPrincipalLocaleAttribute, ActorResolverSupportedLanguages,
    // ActorResolverDefaultLanguage, ActorResolverBrowserLanguageCheck) were removed by the
    // consolidate-locale-config-object change — the four settings are now delivered as a single
    // PrincipalLocaleConfig binding.

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface DispatcherMetricsReturned {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface DispatcherEnableDefaultValidation {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface DispatcherTrimString {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface DispatcherCaseInsensitiveLike {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface IdentifierSignerSecret {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface MetricsCollectorConsumer {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface MetricsCollectorEnabled {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface MetricsCollectorVerbose {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface PayloadValidatorRequiredStringValidatorOption {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface ThreadContextDebugThreadFork {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface ThreadContextInheritableContext {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface RdbmsSequenceStart {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface RdbmsSequenceIncrement {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface RdbmsSequenceCreateIfNotExists {}
}
