package hu.blackbelt.judo.runtime.core.guice;

import com.google.inject.Inject;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.Set;

@ApplicationPath("Calc")
public class CalcApplication extends Application {

    @Inject
    CalcService calcService;

    public void setCalcService(CalcService calcService) {
        this.calcService = calcService;
    }

    @Override
    public Set<Object> getSingletons() {
        return Set.of(calcService);
    }
}
