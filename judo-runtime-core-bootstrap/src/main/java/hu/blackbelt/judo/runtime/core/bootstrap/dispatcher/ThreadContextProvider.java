package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.context.ThreadContext;

public class ThreadContextProvider implements Provider<Context> {
    public static final String THREAD_CONTEXT_DEBUG_THREAD_FORK = "threadContextDebugThreadFork";
    public static final String THREAD_CONTEXT_INHERITABLE_CONTEXT = "threadContextInheritableContext";
    DataTypeManager dataTypeManager;
    Boolean debugThreadFork;
    Boolean inheritableContext;

    @Inject
    public ThreadContextProvider(DataTypeManager dataTypeManager,
                                 @Named(THREAD_CONTEXT_DEBUG_THREAD_FORK) Boolean debugThreadFork,
                                 @Named(THREAD_CONTEXT_INHERITABLE_CONTEXT) Boolean inheritableContext) {
        this.dataTypeManager = dataTypeManager;
        this.debugThreadFork = debugThreadFork;
        this.inheritableContext = inheritableContext;
    }

    @Override
    public Context get() {
        return new ThreadContext(debugThreadFork, inheritableContext, dataTypeManager);
    }
}
