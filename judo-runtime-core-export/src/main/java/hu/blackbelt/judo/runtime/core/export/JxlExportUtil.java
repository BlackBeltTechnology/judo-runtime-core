package hu.blackbelt.judo.runtime.core.export;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.eclipse.emf.ecore.EClass;
import org.jxls.area.XlsArea;
import org.jxls.command.EachCommand;
import org.jxls.common.CellRef;
import org.jxls.common.Context;
import org.jxls.transform.poi.SelectSheetsForStreamingPoiTransformer;

import java.io.*;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class JxlExportUtil {
    private static String METHOD_PREFIX = "get";

    public static Map<String, Class<?>> getAttributesFromClass(Class clazz) {
        Map<String, Class<?>> attributeNames = new HashMap<>();
        for (Method method : clazz.getDeclaredMethods()) {
            String attributeName = "";
            String methodName = method.getName();

            if (methodName.startsWith(METHOD_PREFIX) && !methodName.equals(METHOD_PREFIX)) {
                char attributeNameFirstLetter = methodName.charAt(METHOD_PREFIX.length());
                if (attributeNameFirstLetter != '_') {
                    attributeName = Character.toLowerCase(attributeNameFirstLetter) + methodName.substring(METHOD_PREFIX.length() + 1);
                }
            }
            if (!attributeName.isBlank()) {
                Class<?> attributeType = getMethodReturnType(method);
                attributeNames.put(attributeName, attributeType);
            }
        }
        return attributeNames;
    }

    private static Class<?> getMethodReturnType(Method method) {
        Class<?> attributeType = method.getReturnType();
        if (attributeType.equals(Optional.class)) {
            Type genericType = method.getGenericReturnType();
            if (genericType instanceof ParameterizedType) {
                Type[] actualTypeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
                if (actualTypeArguments.length > 0 && actualTypeArguments[0] instanceof Class) {
                    attributeType = (Class<?>) actualTypeArguments[0];
                }
            }
        }
        return attributeType;
    }

    public static Map<String, Class<?>> getAttributesFromModel(AsmModel asmModel, String fqName) {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        EClass clazz = asmUtils.getClassByFQName(fqName).orElseThrow();
        return clazz.getEAllAttributes().stream().collect(Collectors.toMap(
                a -> a.getName(),
                a -> a.getEType().getInstanceClass()));
    }

    private static Collection<Map<String, Object>> transformPayloadList(Collection<Payload> payloadList) {
        Collection<Map<String, Object>> transformed = new ArrayList<>();

        if (payloadList == null) {
            return null;
        }

        for (Payload payload : payloadList) {
            Map<String, Object> entry = new HashMap<>();

            Set<String> keySet = payload.keySet();
            keySet.forEach(key -> {
                Object value = payload.get(key);

                if (value instanceof Optional<?>) {
                    entry.put(key, ((Optional<?>) value).orElseThrow());
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
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        return in;
    }


    public static void createExcelExport(String sheetName, OutputStream outputStream, List<Payload> list, Map<String, Class<?>> targetTypes, List<String> attributes) throws IOException {
        Workbook workbook = new XSSFWorkbook();
        createTemplateSheet(workbook, sheetName, targetTypes, attributes);

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

                Context context = new Context();
                context.putVar("list", transformPayloadList(list));

                // To debug use: headerArea.applyAt(new CellRef(RESULT_SHEET_NAME + "!A1"), context)
                headerArea.applyAt(new CellRef(sheetName + "!A1"), context);
                transformer.write();
        }
    }

    public static InputStream createExcelExportToInputStream(String sheetName, OutputStream outputStream, List<Payload> list, Map<String, Class<?>> targetTypes, List<String> attributes) throws IOException {
        Workbook workbook = new XSSFWorkbook();
        createTemplateSheet(workbook, sheetName, targetTypes, attributes);

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

            Context context = new Context();
            context.putVar("list", transformPayloadList(list));

            // To debug use: headerArea.applyAt(new CellRef(RESULT_SHEET_NAME + "!A1"), context)
            headerArea.applyAt(new CellRef(sheetName + "!A1"), context);
            transformer.write();
        }

        if (outputStream != null) {
            outputStream.write(byteArrayOutputStream.toByteArray());
        }

        return convertOutputStreamIntoInputStream(byteArrayOutputStream);
    }


    private static void createTemplateSheet(Workbook workbook, String templateSheetName, Map<String, Class<?>> targetTypes, List<String> attributes) throws IOException {
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
                createHelper.createDataFormat().getFormat("yyyy.mm.dd."));
        CellStyle dateTimeCellStyle = workbook.createCellStyle();
        dateTimeCellStyle.setDataFormat(
                createHelper.createDataFormat().getFormat("yyyy.mm.dd. hh:mm:ss"));


        for (String attributeName : attributes) {
            Class<?> returnType = targetTypes.get(attributeName);

            if (returnType != null) {
                workbook.getSheet(templateSheetName).getRow(0).createCell(column).setCellValue(attributeName);
                workbook.getSheet(templateSheetName).getRow(1).createCell(column).setCellValue("${context." + attributeName + "}");

                if (returnType.equals(LocalDateTime.class)) {
                    workbook.getSheet(templateSheetName).getRow(1).getCell(column).setCellStyle(dateTimeCellStyle);
                } else if (returnType.equals(LocalDate.class)) {
                    workbook.getSheet(templateSheetName).getRow(1).getCell(column).setCellStyle(dateCellStyle);
                } else if (returnType.equals(LocalTime.class)) {
                    workbook.getSheet(templateSheetName).getRow(1).getCell(column).setCellStyle(timeCellStyle);
                }
                column++;
            }
        }
    }
}
