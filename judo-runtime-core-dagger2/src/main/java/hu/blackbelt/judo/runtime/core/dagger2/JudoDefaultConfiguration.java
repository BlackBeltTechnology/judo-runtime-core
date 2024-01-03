package hu.blackbelt.judo.runtime.core.dagger2;


import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EReference;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.function.Consumer;

@Module
public class JudoDefaultConfiguration {

    public static final String ACTOR_RESOLVER_CHECK_MAPPED_ACTORS = "actorResolverCheckMappedActors";
    public static final String QUERY_FACTORY_CUSTOM_JOIN_DEFINITIONS = "queryFactoryCustomJoinDefinitions";
    public static final String RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED = "rdbmsDaoOptimisticLockEnabled";
    public static final String RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS = "rdbmsDaoMarkSelectedRangeItems";
    public static final String RDBMS_SELECT_CHUNK_SIZE = "rdbmsDaoChunkSize";
    public static final String DISPATCHER_METRICS_RETURNED = "dispatcherMetricsReturned";
    public static final String DISPATCHER_ENABLE_VALIDATION = "dispatcherEnableDefaultValidation";
    public static final String DISPATCHER_TRIM_STRING = "dispatcherTrimString";
    public static final String DISPATCHER_CASE_INSENSITIVE_LIKE = "dispatcherCaseInsensitiveLike";
    public static final String IDENTIFIER_SIGNER_SECRET = "identifierSignerSecret";
    public static final String METRICS_COLLECTOR_CONSUMER = "metricsCollectorConsumer";
    public static final String METRICS_COLLECTOR_ENABLED = "metricsCollectorEnabled";
    public static final String METRICS_COLLECTOR_VERBOSE = "metricsCollectorVerbose";
    public static final String PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION = "payloadValidatorRequiredStringValidatorOption";

    @JudoApplicationScope
    @Provides
    @Named(QUERY_FACTORY_CUSTOM_JOIN_DEFINITIONS)
    @Nullable
    EMap<EReference, CustomJoinDefinition> providesQuerFactoryCustomJoinDefinitions() {
        return null;
    }

    @JudoApplicationScope
    @Provides
    @Named(RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED)
    @Nullable
    Boolean providesRdbmsDaoOptimisticLockEnabled() {
        return true;
    }

    @JudoApplicationScope
    @Provides
    @Named(RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS)
    @Nullable
    Boolean providesRdbmsDaoMarkSelectedRangeItems() {
        return false;
    }

    @JudoApplicationScope
    @Provides
    @Named(RDBMS_SELECT_CHUNK_SIZE)
    @Nullable
    Integer providesRdbmsSelectChunkSize() {
        return 1000;
    };

    @JudoApplicationScope
    @Provides
    @Named(ACTOR_RESOLVER_CHECK_MAPPED_ACTORS)
    @Nullable
    Boolean providesActorResolverCheckMappedActors() {
        return false;
    }

    @JudoApplicationScope
    @Provides
    @Named(DISPATCHER_METRICS_RETURNED)
    @Nullable
    Boolean providesDispatcherMetricsReturned() {
        return true;
    }

    @JudoApplicationScope
    @Provides
    @Named(DISPATCHER_ENABLE_VALIDATION)
    @Nullable
    Boolean providesDispatcherEnableValidation() {
        return true;
    }

    @JudoApplicationScope
    @Provides
    @Named(DISPATCHER_TRIM_STRING)
    @Nullable
    Boolean providesDispatcherTrimString() {
        return false;
    }

    @JudoApplicationScope
    @Provides
    @Named(DISPATCHER_CASE_INSENSITIVE_LIKE)
    @Nullable
    Boolean providesDispatcherCaseInsensitiveLike() {
        return false;
    }

    @JudoApplicationScope
    @Provides
    @Named(IDENTIFIER_SIGNER_SECRET)
    @Nullable
    String providesIdentifierSignerSecret() {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @JudoApplicationScope
    @Provides
    @Named(METRICS_COLLECTOR_CONSUMER)
    @Nullable
    Consumer providesMetricsConsumer() {
        return (m) -> {};
    };

    @JudoApplicationScope
    @Provides
    @Named(METRICS_COLLECTOR_ENABLED)
    @Nullable
    Boolean providesMetricsCollectorEnabled() {
        return false;
    };

    @JudoApplicationScope
    @Provides
    @Named(METRICS_COLLECTOR_VERBOSE)
    @Nullable
    Boolean  providesMetricsCollectorVerbose() {
        return false;
    }

    @JudoApplicationScope
    @Provides
    @Named(PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION)
    @Nullable
    String providesPayloadValidatorRequiredStringValidatorOption() {
        return "ACCEPT_NON_EMPTY";
    }

}
