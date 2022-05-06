package hu.blackbelt.judo.runtime.core.bootstrap.dao;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.processors.PayloadDaoProcessor;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import org.eclipse.emf.ecore.resource.ResourceSet;

public class PayloadDaoProcessorInjector extends PayloadDaoProcessor {

    @Inject
    @Override
    public void setIdentifierProvider(IdentifierProvider identifierProvider) {
        super.setIdentifierProvider(identifierProvider);
    }

    @Inject
    @Override
    public void setResourceSet(@Named("ASM") ResourceSet resourceSet) {
        super.setResourceSet(resourceSet);
    }

    @Inject
    @Override
    public void setQueryFactory(QueryFactory queryFactory) {
        super.setQueryFactory(queryFactory);
    }

    @Inject
    @Override
    public void setInstanceCollector(InstanceCollector instanceCollector) {
        super.setInstanceCollector(instanceCollector);
    }
}
