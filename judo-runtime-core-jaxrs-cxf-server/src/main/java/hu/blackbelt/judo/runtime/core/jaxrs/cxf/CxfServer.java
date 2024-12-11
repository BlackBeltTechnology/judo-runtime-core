package hu.blackbelt.judo.runtime.core.jaxrs.cxf;

import org.apache.cxf.binding.BindingFactoryManager;
import org.apache.cxf.jaxrs.JAXRSBindingFactory;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;

import javax.ws.rs.core.Application;
import javax.ws.rs.ext.RuntimeDelegate;

public class CxfServer {

    public void registerServer(Application application) {
        RuntimeDelegate delegate = RuntimeDelegate.getInstance();
        JAXRSServerFactoryBean bean = delegate.createEndpoint(application, JAXRSServerFactoryBean.class);
    }

    public void configure(Integer port, Application application) {
        /*
        JAXRSServerFactoryBean sf = new JAXRSServerFactoryBean();
        sf.setResourceClasses(CustomerService.class);
        sf.setResourceProvider(CustomerService.class, new SingletonResourceProvider(new CustomerService()));
        sf.setAddress("http://localhost:9000/");
        BindingFactoryManager manager = sf.getBus().getExtension(BindingFactoryManager.class);
        JAXRSBindingFactory factory = new JAXRSBindingFactory();
        factory.setBus(sf.getBus());
        manager.registerBindingFactory(JAXRSBindingFactory.JAXRS_BINDING_ID, factory);
        sf.create();
        */
    }
}
