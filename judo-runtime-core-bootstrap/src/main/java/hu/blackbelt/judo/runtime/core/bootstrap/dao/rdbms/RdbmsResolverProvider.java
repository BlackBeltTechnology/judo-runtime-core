package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

public class RdbmsResolverProvider implements Provider<RdbmsResolver> {

    @Inject
    JudoModelHolder models;

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
