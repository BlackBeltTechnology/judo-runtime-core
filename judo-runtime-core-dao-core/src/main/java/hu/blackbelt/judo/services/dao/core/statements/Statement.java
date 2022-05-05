package hu.blackbelt.judo.services.dao.core.statements;

import hu.blackbelt.judo.services.dao.core.values.InstanceValue;
import lombok.*;
import org.eclipse.emf.ecore.EReference;

import java.util.Optional;

@Getter
@AllArgsConstructor
public abstract class Statement<ID> {

    @NonNull
    InstanceValue<ID> instance;

}
