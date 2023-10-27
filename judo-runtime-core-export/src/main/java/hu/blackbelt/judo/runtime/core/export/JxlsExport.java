package hu.blackbelt.judo.runtime.core.export;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dispatcher.Export;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class JxlsExport implements Export {

    @Override
    public void exportToOutputStream(OutputStream output, String type, List<Payload> list, List<String> attributes, Class clazz) throws IOException {
        JxlExportUtil.createExcelExport(clazz.getSimpleName(), output, list, JxlExportUtil.getAttributesFromClass(clazz), attributes);
    }

    @Override
    public void exportToOutputStream(OutputStream output, String type, List<Payload> list, List<String> attributes, AsmModel asmModel, String fqName) throws IOException {
        JxlExportUtil.createExcelExport(fqName, output, list, JxlExportUtil.getAttributesFromModel(asmModel, fqName), attributes);
    }

    @Override
    public InputStream exportToInputStream(String type, List<Payload> list, List<String> attributes, OutputStream output, Class clazz) throws IOException {
        return JxlExportUtil.createExcelExportToInputStream(clazz.getSimpleName(), output, list, JxlExportUtil.getAttributesFromClass(clazz), attributes);
    }

    @Override
    public InputStream exportToInputStream(String type, List<Payload> list, List<String> attributes, OutputStream output, AsmModel asmModel, String fqName) throws IOException {
        return JxlExportUtil.createExcelExportToInputStream(fqName, output, list, JxlExportUtil.getAttributesFromModel(asmModel, fqName), attributes);
    }
}
