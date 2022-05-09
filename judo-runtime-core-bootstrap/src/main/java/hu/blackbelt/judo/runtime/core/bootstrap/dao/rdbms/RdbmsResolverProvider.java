package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

public class RdbmsResolverProvider implements Provider<RdbmsResolver> {

    @Inject
    JudoModelSpecification models;

    @Inject
    TransformationTraceService transformationTraceService;

    @Override
    public RdbmsResolver get() {
        return RdbmsResolver.builder()
                .asmModel(models.getAsmModel())
                .transformationTraceService(transformationTraceService)
                .build();
    }
}
