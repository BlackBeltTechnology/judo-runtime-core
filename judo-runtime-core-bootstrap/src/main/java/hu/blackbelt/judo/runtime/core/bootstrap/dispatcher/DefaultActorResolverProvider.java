package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;

public class DefaultActorResolverProvider implements Provider<ActorResolver> {

    public static final String ACTOR_RESOLVER_CHECK_MAPPED_ACTORS = "actorResolverCheckMappedActors";

    @Inject
    AsmModel asmModel;

    @SuppressWarnings("rawtypes")
	@Inject
    DAO dao;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject(optional = true)
    @Named(ACTOR_RESOLVER_CHECK_MAPPED_ACTORS)
    Boolean checkMappedActors = false;

    @SuppressWarnings("unchecked")
	@Override
    public ActorResolver get() {
        return DefaultActorResolver.builder()
                .dataTypeManager(dataTypeManager)
                .dao(dao)
                .asmModel(asmModel)
                .checkMappedActors(checkMappedActors)
                .build();
    }
}
