package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

import com.google.common.collect.Maps;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.UUIDIdentifierProvider;
import hu.blackbelt.judo.runtime.core.dao.core.statements.AddReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.ReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.rdbms.*;
import hu.blackbelt.judo.tatami.core.EMapWrapper;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.ECollections;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j(topic = "dao-rdbms")
public abstract class StatementExecutor<ID> {

    public static final String ID_COLUMN_NAME = "ID";
    public static final String NAME_COLUMN_NAME = "C_NAME";

    public static final String ENTITY_TYPE_COLUMN_NAME = "TYPE";
    public static final String ENTITY_TYPE_MAP_KEY = "__entityType";

    public static final String ENTITY_VERSION_COLUMN_NAME = "VERSION";
    public static final String ENTITY_VERSION_MAP_KEY = "__version";

    public static final String ENTITY_CREATE_USERNAME_COLUMN_NAME = "CREATE_USERNAME";
    public static final String ENTITY_CREATE_USERNAME_MAP_KEY = "__createdBy";

    public static final String ENTITY_CREATE_USER_ID_COLUMN_NAME = "CREATE_USER_ID";
    public static final String ENTITY_CREATE_USER_ID_MAP_KEY = "__createdById";

    public static final String ENTITY_CREATE_TIMESTAMP_COLUMN_NAME = "CREATE_TIMESTAMP";
    public static final String ENTITY_CREATE_TIMESTAMP_MAP_KEY = "__createTimestamp";

    public static final String ENTITY_UPDATE_USERNAME_COLUMN_NAME = "UPDATE_USERNAME";
    public static final String ENTITY_UPDATE_USERNAME_MAP_KEY = "__updatedBy";

    public static final String ENTITY_UPDATE_USER_ID_COLUMN_NAME = "UPDATE_USER_ID";
    public static final String ENTITY_UPDATE_USER_ID_MAP_KEY = "__updatedById";

    public static final String ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME = "UPDATE_TIMESTAMP";
    public static final String ENTITY_UPDATE_TIMESTAMP_MAP_KEY = "__updateTimestamp";

    public static final String SELECTED_ITEM_KEY = "__selected";

    @NonNull
    AsmModel asmModel;

    @NonNull
    RdbmsModel rdbmsModel;

    @NonNull
    TransformationTraceService transformationTraceService;

    @Builder.Default
    IdentifierProvider<ID> identifierProvider = (IdentifierProvider<ID>) new UUIDIdentifierProvider();

    @NonNull
    Dialect dialect;

    @NonNull
    Coercer coercer;

    @NonNull
    RdbmsParameterMapper rdbmsParameterMapper;

    @Getter
    private final RdbmsReferenceUtil<ID> rdbmsReferenceUtil;

    public StatementExecutor(@NonNull AsmModel asmModel,
                             @NonNull RdbmsModel rdbmsModel,
                             @NonNull TransformationTraceService transformationTraceService,
                             @NonNull RdbmsParameterMapper rdbmsParameterMapper,
                             @NonNull Coercer coercer,
                             IdentifierProvider<ID> identifierProvider,
                             Dialect dialect) {
        this.asmModel = asmModel;
        this.rdbmsModel = rdbmsModel;
        this.transformationTraceService = transformationTraceService;
        this.dialect = dialect;
        this.rdbmsParameterMapper =  rdbmsParameterMapper;
        this.identifierProvider = identifierProvider;
        this.coercer = coercer;
        rdbmsReferenceUtil = new RdbmsReferenceUtil<>(asmModel, rdbmsModel, transformationTraceService);
    }


    /**
     * Extracting reference informations from {@link AddReferenceStatement} instances.
     *
     * @param referenceStatements
     * @return
     */
    protected Stream<RdbmsReference<ID>> collectRdbmsReferencesReferenceStatements(Collection<ReferenceStatement<ID>> referenceStatements) {

        return Stream.concat(referenceStatements.stream()
                .map(addReferenceStatement ->
                        rdbmsReferenceUtil.buildRdbmsReferenceForStatement(
                                RdbmsReference.rdbmsReferenceBuilder()
                                        .statement(addReferenceStatement)
                                        .identifier(addReferenceStatement.getIdentifier())
                                        .oppositeIdentifier(addReferenceStatement.getInstance().getIdentifier())
                                        .reference(addReferenceStatement.getReference())
                        )
                ),
                referenceStatements.stream().filter(r -> r.getReference().getEOpposite() != null)
                        .map(addReferenceStatement ->
                                rdbmsReferenceUtil.buildRdbmsReferenceForStatement(
                                        RdbmsReference.rdbmsReferenceBuilder()
                                                .statement(addReferenceStatement)
                                                .identifier(addReferenceStatement.getInstance().getIdentifier())
                                                .oppositeIdentifier(addReferenceStatement.getIdentifier())
                                                .reference(addReferenceStatement.getReference().getEOpposite())
                                )
                        )
                );
    }

    /**
     * Collecting the mandatory/optional references with values for the given update relation statement.
     * When mandatory flag given madatory references collected,
     * when optional optionals too.
     *
     * @param identifier
     * @param referenceStatements
     * @param mandatory
     * @param optional
     * @return
     */
    protected Map<RdbmsReference<ID>, ID> collectReferenceIdentifiersForGivenIdentifier(
            ID identifier,
            Collection<ReferenceStatement<ID>> referenceStatements,
            boolean mandatory,
            boolean optional) {

        RdbmsResolver rdbms = new RdbmsResolver(asmModel, transformationTraceService);
        Map<RdbmsReference<ID>, ID> referenceMap = new EMapWrapper(ECollections.asEMap(Maps.newHashMap()));

        collectRdbmsReferencesReferenceStatements(referenceStatements)
                .filter(rdbmsReference -> rdbmsReference.getRule().isForeignKey() || rdbmsReference.getRule().isInverseForeignKey())
                .forEach(rdbmsReference -> {
                    boolean referenceIsMandatory = rdbms.rdbmsField(rdbmsReference.getReference()).isMandatory();
                    if ((referenceIsMandatory && mandatory) || (!referenceIsMandatory && optional)) {
                        if (rdbmsReference.getRule().isForeignKey()
                                && rdbmsReference.getIdentifier().equals(identifier)) {
                            referenceMap.put(
                                    rdbmsReference,
                                    rdbmsReference.getOppositeIdentifier());
                        } else if (rdbmsReference.getRule().isInverseForeignKey()
                                && rdbmsReference.getOppositeIdentifier().equals(identifier)) {
                            referenceMap.put(
                                    rdbmsReference,
                                    rdbmsReference.getIdentifier());
                        }
                    }
                });
        return referenceMap;
    }

}
