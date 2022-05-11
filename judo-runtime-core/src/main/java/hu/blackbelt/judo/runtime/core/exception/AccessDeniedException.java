package hu.blackbelt.judo.runtime.core.exception;

public class AccessDeniedException extends ClientException {

    private final FeedbackItem feedbackItem;

    public AccessDeniedException(FeedbackItem feedbackItem) {
        super();
        this.feedbackItem = feedbackItem;
    }

    @Override
    public Integer getStatusCode() {
        return 403;
    }

    @Override
    public Object getDetails() {
        return feedbackItem;
    }
}
