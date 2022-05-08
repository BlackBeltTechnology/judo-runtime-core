package hu.blackbelt.judo.runtime.core.bootstrap.dao;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.processors.PayloadDaoProcessor;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import org.eclipse.emf.ecore.resource.ResourceSet;

public class PayloadDaoProcessorProvider implements Provider<PayloadDaoProcessor> {

    IdentifierProvider identifierProvider;
    AsmModel asmModel;
    QueryFactory queryFactor;
    InstanceCollector instanceCollector;

    @Inject
    public PayloadDaoProcessorProvider(IdentifierProvider identifierProvider,
                                       AsmModel asmModel,
                                       QueryFactory queryFactory,
                                       InstanceCollector instanceCollector) {
        this.identifierProvider = identifierProvider;
        this.asmModel = asmModel;
        this.queryFactor = queryFactory;
        this.instanceCollector = instanceCollector;

    }

    @Override
    public PayloadDaoProcessor get() {
        return new PayloadDaoProcessor(asmModel.getResourceSet(), identifierProvider, queryFactor, instanceCollector);
    }
}
