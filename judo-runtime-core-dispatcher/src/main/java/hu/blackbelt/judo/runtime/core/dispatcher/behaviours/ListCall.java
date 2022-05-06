package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.mapper.api.Coercer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import javax.transaction.TransactionManager;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class ListCall<ID> extends AlwaysRollbackTransactionalBehaviourCall {

    final DAO dao;
    final AsmUtils asmUtils;
    final IdentifierProvider<ID> identifierProvider;
    final Coercer coercer;
    final ActorResolver actorResolver;

    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    public ListCall(Context context, DAO dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils, final TransactionManager transactionManager, final Coercer coercer, final ActorResolver actorResolver, boolean caseInsensitiveLike) {
        super(context, transactionManager);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
        this.coercer = coercer;
        this.actorResolver = actorResolver;
        queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor(asmUtils, caseInsensitiveLike, identifierProvider, coercer);
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.LIST).isPresent();
    }

    @Override
    public Object callInRollbackTransaction(final Map<String, Object> exchange, final EOperation operation) {
        final boolean bound = asmUtils.isBound(operation);
        final EReference owner = (EReference) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        Object result;

        final Optional<Map<String, Object>> queryCustomizerParameter = operation.getEParameters().stream().map(p -> p.getName())
                .findFirst()
                .map(inputParameter -> (Map<String, Object>) exchange.get(inputParameter));

        final DAO.QueryCustomizer queryCustomizer = queryCustomizerParameterProcessor.build(queryCustomizerParameter.orElse(null), owner.getEReferenceType());

        if (AsmUtils.annotatedAsTrue(owner, "access") && owner.isDerived() && asmUtils.isMappedTransferObjectType(owner.getEContainingClass())) {
            checkArgument(!bound, "Operation must be unbound");

            final Map<String, Object> actor;
            if (exchange.containsKey(Dispatcher.ACTOR_KEY)) {
                actor = (Map<String, Object>) exchange.get(Dispatcher.ACTOR_KEY);
            } else if (exchange.get(Dispatcher.PRINCIPAL_KEY) instanceof JudoPrincipal) {
                actor = actorResolver.authenticateByPrincipal((JudoPrincipal) exchange.get(Dispatcher.PRINCIPAL_KEY))
                        .orElseThrow(() -> new IllegalArgumentException("Unknown actor"));
            } else {
                throw new IllegalStateException("Unknown or unsupported actor");
            }

            final ID id = (ID) actor.get(identifierProvider.getName());
            result = extractResult(operation, dao.searchNavigationResultAt(id, owner, queryCustomizer));
        } else if (AsmUtils.annotatedAsTrue(owner, "access") || !asmUtils.isMappedTransferObjectType(owner.getEContainingClass())) {
            checkArgument(!bound, "Operation must be unbound");
            final List<Payload> resultList;
            if (AsmUtils.annotatedAsTrue(owner, "access") && !owner.isDerived()) {
                resultList = dao.search(owner.getEReferenceType(), queryCustomizer);
            } else {
                resultList = dao.searchReferencedInstancesOf(owner, owner.getEReferenceType(), queryCustomizer);
            }
            result = extractResult(operation, resultList);
        } else {
            checkArgument(bound, "Operation must be bound");

            final Optional<Optional<Object>> resultInThis = Optional.ofNullable(exchange.get(Dispatcher.INSTANCE_KEY_OF_BOUND_OPERATION))
                    .filter(_this -> _this instanceof Payload && ((Payload) _this).containsKey(owner.getName()))
                    .map(_this -> Optional.ofNullable(((Payload) _this).get(owner.getName())));

            if (resultInThis.isPresent()) {
                result = resultInThis.get().orElse(null);
            } else {
                result = extractResult(operation, dao.searchNavigationResultAt(exchange.get(identifierProvider.getName()), owner, queryCustomizer));
            }
        }
        return result;
    }

    private Object extractResult(final EOperation operation, final List<Payload> resultList) {
        if (operation.isMany()) {
            return resultList;
        } else {
            if (operation.isRequired()) {
                checkArgument(!resultList.isEmpty(), "No record found");
                return resultList.get(0);
            } else {
                return resultList.isEmpty() ? null : resultList.get(0);
            }
        }
    }
}
