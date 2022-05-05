package hu.blackbelt.judo.services.core.exception;

public class AuthenticationRequiredException extends ClientException {

    private FeedbackItem feedbackItem;

    public AuthenticationRequiredException(FeedbackItem feedbackItem) {
        super();
        this.feedbackItem = feedbackItem;
    }

    @Override
    public Integer getStatusCode() {
        return 401;
    }

    @Override
    public Object getDetails() {
        return feedbackItem;
    }
}
