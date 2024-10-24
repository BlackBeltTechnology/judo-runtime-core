package hu.blackbelt.judo.runtime.core.dispatcher;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Locale;

public class UnsupportedExportImpl implements Export {


    @Override
    public void exportToOutputStream(OutputStream output, String type, List<Payload> list, List<String> attributes, Class clazz, Locale locale) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void exportToOutputStream(OutputStream output, String type, List<Payload> list, List<String> attributes, AsmModel asmModel, String fqName, Locale locale) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream exportToInputStream(String type, List<Payload> list, List<String> attributes, OutputStream output, Class clazz, Locale locale) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream exportToInputStream(String type, List<Payload> list, List<String> attributes, OutputStream output, AsmModel asmModel, String fqName, Locale locale) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }
}
