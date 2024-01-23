package hu.blackbelt.judo.runtime.core.dispatcher;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import org.osgi.annotation.versioning.ProviderType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface Export {

    void exportToOutputStream(OutputStream output, String type, List<Payload> list, List<String> attributes, Class clazz) throws IOException;

    void exportToOutputStream(OutputStream output, String type, List<Payload> list, List<String> attributes, AsmModel asmModel, String fqName) throws IOException;

    InputStream exportToInputStream(String type, List<Payload> list, List<String> attributes, OutputStream output, Class clazz) throws IOException;

    InputStream exportToInputStream(String type, List<Payload> list, List<String> attributes, OutputStream output, AsmModel asmModel, String fqName) throws IOException;

}
