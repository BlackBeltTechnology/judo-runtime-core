package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultActorResolver;
import lombok.NonNull;

public class DefaultActorResolverInjector  extends DefaultActorResolver {

    @Inject
    @Override
    public void setAsmModel(@NonNull AsmModel asmModel) {
        super.setAsmModel(asmModel);
    }

    @Inject
    @Override
    public void setDao(@NonNull DAO dao) {
        super.setDao(dao);
    }

    @Inject
    @Override
    public void setDataTypeManager(@NonNull DataTypeManager dataTypeManager) {
        super.setDataTypeManager(dataTypeManager);
    }
}
