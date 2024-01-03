package hu.blackbelt.judo.runtime.core.dagger2;

import dagger.Module;
import dagger.Provides;

import javax.annotation.Nullable;
import javax.inject.Named;

@Module
public class UtilsConfiguration {
    public static final String THREAD_CONTEXT_DEBUG_THREAD_FORK = "threadContextDebugThreadFork";
    public static final String THREAD_CONTEXT_INHERITABLE_CONTEXT = "threadContextInheritableContext";

    @JudoApplicationScope
    @Provides
    @Named(THREAD_CONTEXT_DEBUG_THREAD_FORK)
    @Nullable
    Boolean providesThreadContextDebugThreadFork() {
        return false;
    }

    @JudoApplicationScope
    @Provides
    @Named(THREAD_CONTEXT_INHERITABLE_CONTEXT)
    @Nullable
    Boolean providesThreadContextInheritableContext() {
        return true;
    }

}
