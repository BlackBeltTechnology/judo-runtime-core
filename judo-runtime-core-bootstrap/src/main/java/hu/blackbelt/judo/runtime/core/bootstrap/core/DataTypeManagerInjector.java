package hu.blackbelt.judo.runtime.core.bootstrap.core;

import com.google.inject.Inject;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import lombok.NonNull;

public class DataTypeManagerInjector extends DataTypeManager {

    @Inject
    @Override
    public void setCoercer(@NonNull ExtendableCoercer coercer) {
        super.setCoercer(coercer);
    }

}
