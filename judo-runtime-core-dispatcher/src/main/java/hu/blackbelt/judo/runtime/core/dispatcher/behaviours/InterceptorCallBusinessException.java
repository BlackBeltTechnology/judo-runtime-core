package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dispatcher.api.BusinessException;

import java.util.Locale;
import java.util.Map;

public class InterceptorCallBusinessException extends BusinessException {
    public InterceptorCallBusinessException(String type, String errorCode, Map<String, Object> details, Throwable throwable, Locale locale) {
        super(type, errorCode, details, throwable, locale);
    }
}
