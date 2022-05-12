package hu.blackbelt.judo.runtime.core.exception;

import lombok.Getter;

import java.util.Collection;

public class ValidationException extends ClientException {

	private static final long serialVersionUID = 1550702685342981741L;

	@Getter
    private final Collection<FeedbackItem> feedbackItems;

    public ValidationException(final Collection<FeedbackItem> feedbackItems) {
        super();
        this.feedbackItems = feedbackItems;
    }

    public ValidationException(final String message, final Collection<FeedbackItem> feedbackItems) {
        super(message);
        this.feedbackItems = feedbackItems;
    }

    @Override
    public Integer getStatusCode() {
        return 400;
    }

    @Override
    public Object getDetails() {
        return feedbackItems;
    }
}
