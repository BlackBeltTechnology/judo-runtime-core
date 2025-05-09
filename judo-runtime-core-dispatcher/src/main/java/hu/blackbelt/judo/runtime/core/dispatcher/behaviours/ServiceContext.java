package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.Serializable;

@Builder
@Getter
public class ServiceContext {

    final PlatformTransactionManager transactionManager;
    final OperationCallInterceptorProvider interceptorProvider;
    final DAO dao;
    final IdentifierProvider identifierProvider;
    final AsmModel asmModel;
    final AsmUtils asmUtils;
    final Coercer coercer;
    final ActorResolver actorResolver;
    final Context context;
    final boolean caseInsensitiveLike;

}
