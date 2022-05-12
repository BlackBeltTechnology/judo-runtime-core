package hu.blackbelt.judo.runtime.core.exception;

public class NotFoundException extends ClientException {

	private static final long serialVersionUID = -153898287660056859L;
	private final FeedbackItem feedbackItem;

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
