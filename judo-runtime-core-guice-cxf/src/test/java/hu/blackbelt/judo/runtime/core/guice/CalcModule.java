package hu.blackbelt.judo.runtime.core.guice;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

import javax.ws.rs.core.Application;

public class CalcModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(CalcService.class).toInstance(new CalcService());

        Multibinder<Application> applicationMultibinder = Multibinder.newSetBinder(binder(), Application.class);
        applicationMultibinder.addBinding().toInstance(new CalcApplication());
    }
}
