package hu.blackbelt.judo.runtime.core.dao.core.statements;

import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;
import lombok.*;

@Getter
@AllArgsConstructor
public abstract class Statement<ID> {

    @NonNull
    InstanceValue<ID> instance;

}
