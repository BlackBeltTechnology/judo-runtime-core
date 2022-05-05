package hu.blackbelt.judo.services.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.mapper.api.Coercer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import javax.transaction.TransactionManager;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport.LoadArguments.expressionLoadArgumentsBuilder;
import static hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport.loadExpression;

@Slf4j
public class GetReferenceRangeCall<ID> extends AlwaysRollbackTransactionalBehaviourCall {

    final DAO dao;
    final AsmUtils asmUtils;
    final ExpressionModelResourceSupport expressionModelResourceSupport;
    final IdentifierProvider<ID> identifierProvider;

    private final MarkedIdRemover<ID> markedIdRemover;
    private final CollectedIdRemover<ID> collectedIdRemover;

    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    private static final String OWNER_KEY = "owner";
    private static final String QUERY_CUSTOMIZER_KEY = "queryCustomizer";

    @SneakyThrows
    public GetReferenceRangeCall(Context context, DAO dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils, ExpressionModel expressionModel, TransactionManager transactionManager, Coercer coercer, boolean caseInsensitiveLike) {
        super(context, transactionManager);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
        this.markedIdRemover = new MarkedIdRemover<>(identifierProvider.getName());
        this.collectedIdRemover = new CollectedIdRemover<>(identifierProvider.getName());

        this.expressionModelResourceSupport = loadExpression(expressionLoadArgumentsBuilder()
                .resourceSet(expressionModel.getResourceSet())
                .uri(URI.createURI("expression:internal-range-" + expressionModel.getName()))
                .build());

        this.queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor(asmUtils, caseInsensitiveLike, identifierProvider, coercer);
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_REFERENCE_RANGE).isPresent();
    }

    @Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        final EReference owner = (EReference) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final Optional<String> inputParameterName = operation.getEParameters().stream().map(p -> p.getName()).findFirst();
        final Map<String, Object> inputData = (Map<String, Object>) exchange.get(inputParameterName.get());

        final boolean bound = asmUtils.isBound(operation);
        checkArgument(!bound, "Operation must be unbound");

        final DAO.QueryCustomizer queryCustomizer = queryCustomizerParameterProcessor.build(inputData != null ? (Map<String, Object>) inputData.get(QUERY_CUSTOMIZER_KEY) : null, owner.getEReferenceType());

        final Collection<ID> idsToRemove = new HashSet<>();
        final Collection<Payload> result = dao.getRangeOf(owner, inputParameterName.map(parameterName -> exchange.get(parameterName) != null ? Payload.asPayload((Map<String, Object>) exchange.get(parameterName)).getAsPayload(OWNER_KEY) : null).orElse(null), queryCustomizer);

        // collect IDs that are created (temporary)
        result.forEach(p -> markedIdRemover.processAndCollect(p, idsToRemove));
        // remove identifiers of temporary instances (keep identifiers of instances existing before operation call)
        result.forEach(payload -> collectedIdRemover.removeIdentifiers(payload, idsToRemove));

        return result;
    }
}
