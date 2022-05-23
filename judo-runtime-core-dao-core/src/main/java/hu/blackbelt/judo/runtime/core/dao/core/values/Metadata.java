package hu.blackbelt.judo.runtime.core.dao.core.values;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.OffsetDateTime;

@Getter
@Builder(builderMethodName = "buildMetadata")
@ToString
public class Metadata<ID> {

    ID userId;
    String username;
    OffsetDateTime timestamp;
}
