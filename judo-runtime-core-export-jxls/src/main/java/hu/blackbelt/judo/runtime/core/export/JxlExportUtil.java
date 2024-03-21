package hu.blackbelt.judo.runtime.core.export;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.FileType;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.eclipse.emf.ecore.*;
import org.jxls.area.XlsArea;
import org.jxls.command.EachCommand;
import org.jxls.common.CellRef;
import org.jxls.common.Context;
import org.jxls.transform.poi.SelectSheetsForStreamingPoiTransformer;

import java.io.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class JxlExportUtil {
    private static String METHOD_PREFIX = "get";

    public static Map<String, EClassifier> getAttributesFromModel(AsmModel asmModel, String fqName) {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        EClass clazz = asmUtils.getClassByFQName(fqName).orElseThrow();
        return clazz
                .getEAllAttributes()
                .stream()
                .collect(Collectors.toMap(
                        ENamedElement::getName,
                        ETypedElement::getEType)
                );
    }

    private static final Object convertEnumerationValue(final AsmUtils asmUtils, final EDataType dataType, final Integer oldValue) {
        return Optional.ofNullable(asmUtils.all(EEnum.class)
                        .filter(e -> AsmUtils.equals(e, dataType))
                        .findAny()
                        .orElseThrow(() -> new IllegalStateException("Invalid enumeration type: " + AsmUtils.getClassifierFQName(dataType)))
                        .getEEnumLiteral(oldValue))
                .map(l -> l.getLiteral())
                .orElseThrow(() -> new IllegalArgumentException("Invalid enumeration value '" + oldValue + "' of type: " + AsmUtils.getClassifierFQName(dataType)));
    }

    private static Collection<Map<String, Object>> transformPayloadList(final AsmUtils asmUtils, Collection<Payload> payloadList, Map<String, EClassifier> targetTypes) {
        Collection<Map<String, Object>> transformed = new ArrayList<>();

        if (payloadList == null) {
            return null;
        }

        for (Payload payload : payloadList) {
            Map<String, Object> entry = new HashMap<>();

            Set<String> keySet = payload.keySet();
            keySet.forEach(key -> {
                Object value = payload.get(key);
                if (value != null && targetTypes.containsKey(key) && targetTypes.get(key) instanceof EEnum) {
                    value = convertEnumerationValue(asmUtils, (EDataType)targetTypes.get(key), (Integer)value);
                }

                if (value != null && targetTypes.containsKey(key) && asmUtils.isByteArray((EDataType) targetTypes.get(key))) {
                    value = ((FileType) value).getFileName();
                }

                if (value != null && targetTypes.containsKey(key)
                        && targetTypes.get(key).getInstanceClass() != null
                        && targetTypes.get(key).getInstanceClass().equals(Double.class)) {
                    String doubleValue = value.toString();
                    value = new BigDecimal(doubleValue);
                }

                if (value != null && targetTypes.containsKey(key)
                        && targetTypes.get(key).getInstanceClass() != null
                        && targetTypes.get(key).getInstanceClass().equals(LocalDateTime.class)) {
                    ZoneId utcZone = ZoneId.of("UTC");
                    ZoneId localZone = ZoneId.systemDefault();
                    ZonedDateTime utcZonedDateTime = ((LocalDateTime) value).atZone(utcZone);
                    value = utcZonedDateTime.withZoneSameInstant(localZone);
                }

                if (value != null && targetTypes.containsKey(key)
                        && targetTypes.get(key).getInstanceClass() != null
                        && targetTypes.get(key).getInstanceClass().equals(Float.class)) {
                    String floatValue = value.toString();
                    value = new BigDecimal(floatValue);
                }

                if (value instanceof Optional<?>) {
                    entry.put(key, ((Optional<?>) value).orElse(null));
                } else {
                    entry.put(key, value);
                }
            });
            transformed.add(entry);
        }
        return transformed;
    }

    private static InputStream convertOutputStreamIntoInputStream (ByteArrayOutputStream outputStream) throws IOException {
        PipedInputStream in = new PipedInputStream();
        final PipedOutputStream out = new PipedOutputStream(in);
        new Thread(() -> {
            try {
                outputStream.writeTo(out);
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        return in;
    }

    public static void createExcelExport(AsmModel asmModel, String sheetName, OutputStream outputStream, List<Payload> list, Map<String, EClassifier> targetTypes, List<String> attributes) throws IOException {
        Workbook workbook = new XSSFWorkbook();
        createTemplateSheet(workbook, sheetName, targetTypes, attributes, list.size() == 0);

        if (targetTypes.size() > 0) {
                char column = ((char) ((int) 'A' + targetTypes.size()));
                int columns = 0;

                if (list != null) {
                    columns = list.size();
                }

                SelectSheetsForStreamingPoiTransformer transformer = new SelectSheetsForStreamingPoiTransformer(workbook);
                transformer.setOutputStream(outputStream);

                XlsArea headerArea = new XlsArea(sheetName + "!A1:" + column + columns + 1, transformer);
                XlsArea dataArea = new XlsArea(sheetName + "!A2:" + column + "2", transformer);

                EachCommand employeeEachCommand = new EachCommand("context", "list", dataArea);
                headerArea.addCommand("A2:" + column +"2", employeeEachCommand);

                Collection<Map<String, Object>> transformedPayloadList = transformPayloadList(new AsmUtils(asmModel.getResourceSet()), list, targetTypes);
                if (transformedPayloadList != null && transformedPayloadList.size() != 0) {
                    Context context = new Context();
                    context.putVar("list", transformedPayloadList);
                    // To debug use: headerArea.applyAt(new CellRef(RESULT_SHEET_NAME + "!A1"), context)
                    headerArea.applyAt(new CellRef(sheetName + "!A1"), context);
                }

                transformer.write();
        }
    }

    public static InputStream createExcelExportToInputStream(AsmModel asmModel, String sheetName, OutputStream outputStream, List<Payload> list, Map<String, EClassifier> targetTypes, List<String> attributes) throws IOException {
        Workbook workbook = new XSSFWorkbook();
        createTemplateSheet(workbook, sheetName, targetTypes, attributes, list.size() == 0);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        if (targetTypes.size() > 0) {
            char column = ((char) ((int) 'A' + targetTypes.size()));
            int columns = 0;

            if (list != null) {
                columns = list.size();
            }

            SelectSheetsForStreamingPoiTransformer transformer = new SelectSheetsForStreamingPoiTransformer(workbook);
            transformer.setOutputStream(byteArrayOutputStream);

            XlsArea headerArea = new XlsArea(sheetName + "!A1:" + column + columns + 1, transformer);
            XlsArea dataArea = new XlsArea(sheetName + "!A2:" + column + "2", transformer);

            EachCommand employeeEachCommand = new EachCommand("context", "list", dataArea);
            headerArea.addCommand("A2:" + column +"2", employeeEachCommand);

            Collection<Map<String, Object>> transformedPayloadList = transformPayloadList(new AsmUtils(asmModel.getResourceSet()), list, targetTypes);
            if (transformedPayloadList != null && transformedPayloadList.size() != 0) {
                Context context = new Context();
                context.putVar("list", transformedPayloadList);
                // To debug use: headerArea.applyAt(new CellRef(RESULT_SHEET_NAME + "!A1"), context)
                headerArea.applyAt(new CellRef(sheetName + "!A1"), context);
            }

            transformer.write();
        }

        if (outputStream != null) {
            outputStream.write(byteArrayOutputStream.toByteArray());
        }

        return convertOutputStreamIntoInputStream(byteArrayOutputStream);
    }


    private static void createTemplateSheet(Workbook workbook, String templateSheetName, Map<String, EClassifier> targetTypes, List<String> attributes, boolean isEmpty) {
        workbook.createSheet(templateSheetName);
        workbook.getSheet(templateSheetName).createRow(0);
        workbook.getSheet(templateSheetName).createRow(1);

        int column = 0;

        CreationHelper createHelper = workbook.getCreationHelper();
        CellStyle timeCellStyle = workbook.createCellStyle();
        timeCellStyle.setDataFormat(
                createHelper.createDataFormat().getFormat("hh:mm:ss"));
        CellStyle dateCellStyle = workbook.createCellStyle();
        dateCellStyle.setDataFormat(
                createHelper.createDataFormat().getFormat("dd/mm/yyyy"));
        CellStyle dateTimeCellStyle = workbook.createCellStyle();
        dateTimeCellStyle.setDataFormat(
                createHelper.createDataFormat().getFormat("dd/mm/yyyy hh:mm:ss"));
        CellStyle decimalCellStyle = workbook.createCellStyle();
        decimalCellStyle.setDataFormat(
                createHelper.createDataFormat().getFormat("#,##0.00"));
        CellStyle integerCellStyle = workbook.createCellStyle();
        integerCellStyle.setDataFormat(
                createHelper.createDataFormat().getFormat("#,###"));

        for (String attributeName : attributes) {
            EClassifier returnType = targetTypes.get(attributeName);

            if (returnType != null) {
                workbook.getSheet(templateSheetName).getRow(0).createCell(column).setCellValue(attributeName);
                if (isEmpty) {
                    column++;
                    continue;
                }

                workbook.getSheet(templateSheetName).getRow(1).createCell(column).setCellValue("${context." + attributeName + "}");

                if (AsmUtils.isTimestamp((EDataType) returnType)) {
                    workbook.getSheet(templateSheetName).getRow(1).getCell(column).setCellStyle(dateTimeCellStyle);
                } else if (AsmUtils.isDate((EDataType) returnType)) {
                    workbook.getSheet(templateSheetName).getRow(1).getCell(column).setCellStyle(dateCellStyle);
                } else if (AsmUtils.isTime((EDataType) returnType)) {
                    workbook.getSheet(templateSheetName).getRow(1).getCell(column).setCellStyle(timeCellStyle);
                } else if (AsmUtils.isDecimal((EDataType) returnType)) {
                    workbook.getSheet(templateSheetName).getRow(1).getCell(column).setCellStyle(decimalCellStyle);
                } else if (AsmUtils.isInteger((EDataType) returnType)) {
                    workbook.getSheet(templateSheetName).getRow(1).getCell(column).setCellStyle(integerCellStyle);
                }

                column++;
            }
        }
    }
}
