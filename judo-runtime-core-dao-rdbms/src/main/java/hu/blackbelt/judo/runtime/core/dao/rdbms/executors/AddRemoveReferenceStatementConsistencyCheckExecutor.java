package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.AddReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.ReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.RemoveReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Executing {@link RemoveReferenceStatement} statements.
 * It analyzes where the foreign key are presented and making update in the owner table. If it is a join table
 * record is deleted.
 *
 * @param <ID>
 */
class AddRemoveReferenceStatementConsistencyCheckExecutor<ID> extends StatementExecutor<ID> {

    @Builder
    public AddRemoveReferenceStatementConsistencyCheckExecutor(
            @NonNull AsmModel asmModel,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull TransformationTraceService transformationTraceService,
            @NonNull RdbmsParameterMapper<ID> rdbmsParameterMapper,
            @NonNull RdbmsResolver rdbmsResolver,
            @NonNull Coercer coercer,
            @NonNull IdentifierProvider<ID> identifierProvider) {
        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, rdbmsResolver, coercer, identifierProvider);
    }

    /**
     * Collecting {@link RemoveReferenceStatement} statements from statement list and crosschecking
     * it. Checking is there any reference which can cause inconsistency after execution
     * @param jdbcTemplate
     * @param removeReferencesStatements
     */
    public void checkRemoveReferenceStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                               Collection<ReferenceStatement<ID>> removeReferencesStatements,
                                               Collection<ID> idsToDelete) {

        // Check all removed instance - (opposite is single and required)
        Set<ReferenceStatement<ID>> illegalRemoveStatementsSingle = removeReferencesStatements.stream()
                .filter(r -> r.getReference().getEOpposite() != null)
                .filter(r -> !r.getReference().getEOpposite().isMany() && !idsToDelete.contains(r.getIdentifier()) && r.getReference().getEOpposite().isRequired())
                .collect(Collectors.toSet());

        checkState(illegalRemoveStatementsSingle.size() == 0, "There is reference remove which let referrer violate mandatory constraint");

        // TODO: Check all removed instance - (opposite is collection and lower constraint violated by removing the given element)
//        Set<ReferenceStatement<ID>> illegalRemoveStatementsCollection = removeReferencesStatements.stream()
//                .filter(r -> r.getReference().getEOpposite() != null)
//                .filter(r -> r.getReference().getEOpposite().isMany() && r.getReference().getEOpposite().getLowerBound() <= remainingSize)
//                .collect(Collectors.toSet());
//
//        checkState(illegalRemoveStatementsCollection.size() == 0, "There is reference remove which let referrer violate cardinality constraint");
    }

    /**
     * Collecting {@link AddReferenceStatement} statements from statement list and crosschecking
     * it. Checking is there any reference which can cause inconsistency after execution
     * @param jdbcTemplate
     * @param addReferencesStatements
     */
    public void checkAddReferenceStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                            Collection<AddReferenceStatement<ID>> addReferencesStatements) {

        // Check all added instance - (opposite is single and required)
        Set<ReferenceStatement<ID>> illegalAddStatementsSingle = addReferencesStatements.stream()
                .filter(r -> r.getReference().getEOpposite() != null)
                .filter(r -> r.getReference().getEOpposite().getLowerBound() == r.getReference().getEOpposite().getUpperBound())
                .collect(Collectors.toSet());

        checkState(illegalAddStatementsSingle.size() == 0, "There is reference add which let referrer violate mandatory constraint");

        // Check cardinality of back (opposite) references (already set)
        Set<ReferenceStatement<ID>> illegalAddStatementsBecauseOfBackReference = addReferencesStatements.stream()
                .filter(r -> r.getReference().getEOpposite() != null && r.getAlreadyReferencingInstances() != null)
                .filter(r -> r.getReference().getEOpposite().getUpperBound() > -1 && r.getReference().getEOpposite().getUpperBound() <= r.getAlreadyReferencingInstances().size())
                .collect(Collectors.toSet());

        checkState(illegalAddStatementsBecauseOfBackReference.size() == 0, "There is reference add which let back reference violate constraint");

        // TODO: Check all added instance - (opposite is collection and lower constraint violated by adding the given element)
//        Set<ReferenceStatement<ID>> illegalAddStatementsCollection = addReferencesStatements.stream()
//                .filter(r -> r.getReference().getEOpposite() != null)
//                .filter(r -> r.getReference().getEOpposite().isMany() && r.getReference().getEOpposite().getLowerBound() <= remainingSize)
//                .collect(Collectors.toSet());
//
//        checkState(illegalAddStatementsCollection.size() == 0, "There is reference add which let referrer violate cardinality constraint");
    }
}
