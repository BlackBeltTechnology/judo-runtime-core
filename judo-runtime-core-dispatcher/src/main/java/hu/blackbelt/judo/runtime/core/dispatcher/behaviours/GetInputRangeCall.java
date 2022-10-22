package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.mapper.api.Coercer;
import lombok.SneakyThrows;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import org.springframework.transaction.PlatformTransactionManager;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class GetInputRangeCall<ID> extends AlwaysRollbackTransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final AsmUtils asmUtils;
    final ExpressionModelResourceSupport expressionModelResourceSupport;
    final IdentifierProvider<ID> identifierProvider;

    private final MarkedIdRemover<ID> markedIdRemover;
    private final CollectedIdRemover<ID> collectedIdRemover;

    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    private static final String OWNER_KEY = "owner";
    private static final String QUERY_CUSTOMIZER_KEY = "queryCustomizer";

    @SneakyThrows
    public GetInputRangeCall(DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils, ExpressionModel expressionModel, PlatformTransactionManager transactionManager, Coercer coercer, boolean caseInsensitiveLike) {
        super(transactionManager);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
        this.markedIdRemover = new MarkedIdRemover<>(identifierProvider.getName());
        this.collectedIdRemover = new CollectedIdRemover<>(identifierProvider.getName());

        this.expressionModelResourceSupport = ExpressionModelResourceSupport.expressionModelResourceSupportBuilder()
                .resourceSet(expressionModel.getResourceSet())
                .uri(expressionModel.getUri()).build();

        this.queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor<ID>(asmUtils, caseInsensitiveLike, identifierProvider, coercer);
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_INPUT_RANGE).isPresent();
    }

    @Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        final EOperation owner = (EOperation) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));
        String inputRangeReferenceFQName = AsmUtils.getExtensionAnnotationValue(owner, "inputRange", false)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));
        final EReference inputRangeReference = asmUtils.resolveReference(inputRangeReferenceFQName)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final Optional<String> inputParameterName = operation.getEParameters().stream().map(p -> p.getName()).findFirst();
        @SuppressWarnings("unchecked")
		final Map<String, Object> inputData = (Map<String, Object>) exchange.get(inputParameterName.get());

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(!bound, "Operation must be unbound");

        @SuppressWarnings({ "unchecked", "rawtypes" })
		final DAO.QueryCustomizer queryCustomizer = queryCustomizerParameterProcessor.build(inputData != null ? (Map<String, Object>) inputData.get(QUERY_CUSTOMIZER_KEY) : null, inputRangeReference.getEReferenceType());

        final Collection<ID> idsToRemove = new HashSet<>();
        @SuppressWarnings("unchecked")
		final Collection<Payload> result = dao.getRangeOf(inputRangeReference, inputParameterName.map(parameterName -> exchange.get(parameterName) != null ? Payload.asPayload((Map<String, Object>) exchange.get(parameterName)).getAsPayload(OWNER_KEY) : null).orElse(null), queryCustomizer);

        // collect IDs that are created (temporary)
        result.forEach(p -> markedIdRemover.processAndCollect(p, idsToRemove));
        // remove identifiers of temporary instances (keep identifiers of instances existing before operation call)
        result.forEach(payload -> collectedIdRemover.removeIdentifiers(payload, idsToRemove));

        return result;
    }
}
