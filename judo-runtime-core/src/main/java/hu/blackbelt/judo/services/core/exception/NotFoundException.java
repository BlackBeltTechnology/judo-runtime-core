package hu.blackbelt.judo.services.core.exception;

public class NotFoundException extends ClientException {

    private FeedbackItem feedbackItem;

    public NotFoundException(FeedbackItem feedbackItem) {
        super();
        this.feedbackItem = feedbackItem;
    }

    @Override
    public Integer getStatusCode() {
        return 404;
    }

    @Override
    public Object getDetails() {
        return feedbackItem;
    }
}
