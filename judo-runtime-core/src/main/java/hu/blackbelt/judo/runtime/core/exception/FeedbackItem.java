package hu.blackbelt.judo.runtime.core.exception;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import java.util.Map;

@Data
@Builder
public class FeedbackItem {

    private String code;
    private Level level;
    private Object location;

    @Singular
    private Map<String, Object> details;

    public enum Level {
        ERROR, WARNING
    }
}
