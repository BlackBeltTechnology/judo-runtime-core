package hu.blackbelt.judo.runtime.core.guice;

import hu.blackbelt.judo.runtime.core.guice.dispatcher.DefaultPayloadValidatorProvider;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import lombok.*;
import org.eclipse.emf.ecore.EReference;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class JudoDefaultModuleConfiguration {
    public static final JudoDefaultModuleConfiguration DEFAULT = JudoDefaultModuleConfiguration.builder().build();

    @Builder.Default
    Object injectModulesTo = false;
    @Builder.Default
    JudoModelLoader judoModelLoader = null;
    @Builder.Default
    Boolean bindModelHolder = true;
    @Builder.Default
    Map<EReference, CustomJoinDefinition> queryFactoryCustomJoinDefinitions = new ConcurrentHashMap<>();
    @Builder.Default
    Boolean rdbmsDaoOptimisticLockEnabled = true;
    @Builder.Default
    Boolean rdbmsDaoMarkSelectedRangeItems = false;
    @Builder.Default
    Integer rdbmsDaoChunkSize = 1000;
    @Builder.Default
    Boolean actorResolverCheckMappedActors = false;
    @Builder.Default
    Boolean dispatcherMetricsReturned = false;
    @Builder.Default
    Boolean dispatcherEnableDefaultValidation = true;
    @Builder.Default
    Boolean dispatcherCaseInsensitiveLike = false;
    @Builder.Default
    Boolean dispatcherTrimString = false;

    @Builder.Default
    String identifierSignerSecret = null;
    @Builder.Default
    Consumer metricsCollectorConsumer = (m) -> {};
    @Builder.Default
    Boolean metricsCollectorEnabled = false;
    @Builder.Default
    Boolean metricsCollectorVerbose = false;
    @Builder.Default
    String payloadValidatorRequiredStringValidatorOption = DefaultPayloadValidatorProvider.ACCEPT_NON_EMPTY;
    @Builder.Default
    Boolean threadContextDebugThreadFork = false;
    @Builder.Default
    Boolean threadContextInheritableContext = true;
    @Builder.Default
    Long rdbmsSequenceStart = 1L;
    @Builder.Default
    Long rdbmsSequenceIncrement = 1L;
    @Builder.Default
    Boolean rdbmsSequenceCreateIfNotExists = true;
}
