package hu.blackbelt.judo.runtime.core.exception;

public class AccessDeniedException extends ClientException {

	private static final long serialVersionUID = -8619727759658375103L;
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
