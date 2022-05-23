package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.context.ThreadContext;

import javax.annotation.Nullable;

public class ThreadContextProvider implements Provider<Context> {
    public static final String THREAD_CONTEXT_DEBUG_THREAD_FORK = "threadContextDebugThreadFork";
    public static final String THREAD_CONTEXT_INHERITABLE_CONTEXT = "threadContextInheritableContext";

    @Inject
    DataTypeManager dataTypeManager;

    @Inject(optional = true)
    @Named(THREAD_CONTEXT_DEBUG_THREAD_FORK)
    @Nullable
    Boolean debugThreadFork = false;

    @Inject(optional = true)
    @Named(THREAD_CONTEXT_INHERITABLE_CONTEXT)
    @Nullable
    Boolean inheritableContext = true;

    @Override
    public Context get() {
        return new ThreadContext(debugThreadFork, inheritableContext, dataTypeManager);
    }
}
