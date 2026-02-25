# JXLS Excel Export Specification

## Purpose
Provides Excel (XLSX) export functionality for JUDO runtime data using the JXLS template engine and Apache POI, transforming `Payload` collections into formatted spreadsheets with type-aware cell formatting derived from the ASM metamodel.

## Architecture
- **`JxlsExport`** -- Implements the `Export` interface, delegating to `JxlExportUtil` for ASM-model-based Excel export. The Class-based overloads throw `UnsupportedOperationException`.
- **`JxlExportUtil`** -- Static utility that creates XLSX workbooks with JXLS template areas, transforms `Payload` lists into export-ready maps with type conversions (enumerations, decimals, timestamps, file types), and writes the result to an `OutputStream` or returns an `InputStream`.

### Class Relationships
```
JxlsExport (implements Export)
    └── delegates to JxlExportUtil

JxlExportUtil
    ├── getAttributesFromModel(AsmModel, fqName) -> Map<String, EClassifier>
    ├── createExcelExport(AsmModel, sheetName, OutputStream, List<Payload>, targetTypes, attributes)
    ├── createExcelExportToInputStream(AsmModel, sheetName, OutputStream, List<Payload>, targetTypes, attributes)
    ├── transformPayloadList() -- internal type conversion
    └── createTemplateSheet() -- internal XLSX template generation
```

### Package
`hu.blackbelt.judo.runtime.core.export`

## Requirements

### Requirement: ASM model-based attribute type resolution
`JxlExportUtil.getAttributesFromModel()` SHALL resolve all attributes of an EClass identified by its fully qualified name from the `AsmModel` and return a map of attribute name to `EClassifier` type.

#### Scenario: Resolving attributes for a known entity type
- **GIVEN** an `AsmModel` containing an EClass with fully qualified name `"demo.entities.Order"` that has attributes `name` (String) and `amount` (Double)
- **WHEN** `JxlExportUtil.getAttributesFromModel(asmModel, "demo.entities.Order")` is invoked
- **THEN** the returned map contains entries `"name"` and `"amount"` mapped to their respective `EClassifier` types

### Requirement: Enumeration value conversion to literal strings
During payload transformation, `JxlExportUtil` SHALL convert `Integer` enumeration values to their corresponding `EEnum` literal string representations.

#### Scenario: Exporting a payload with an enumeration attribute
- **GIVEN** a `Payload` containing an attribute `status` with integer value `1`, where the ASM model maps this to an `EEnum` with literal `"ACTIVE"` for ordinal 1
- **WHEN** the payload list is transformed by `JxlExportUtil`
- **THEN** the exported map entry for `status` contains the string `"ACTIVE"`

### Requirement: Double and Float conversion to BigDecimal
During payload transformation, `JxlExportUtil` SHALL convert `Double` and `Float` typed attribute values to `BigDecimal` for precision-safe Excel rendering.

#### Scenario: Exporting a payload with a Double attribute
- **GIVEN** a `Payload` containing attribute `price` with value `19.99` and the ASM model type maps to `Double.class`
- **WHEN** the payload list is transformed
- **THEN** the exported map entry for `price` is a `BigDecimal` with value `19.99`

### Requirement: LocalDateTime conversion to local timezone ZonedDateTime
During payload transformation, `JxlExportUtil` SHALL convert `LocalDateTime` values (assumed UTC) to `ZonedDateTime` in the system default timezone for locale-appropriate display in Excel.

#### Scenario: Exporting a payload with a timestamp attribute
- **GIVEN** a `Payload` containing attribute `createdAt` with a `LocalDateTime` of `2024-03-15T10:00:00` and the ASM type is `LocalDateTime.class`
- **WHEN** the payload list is transformed
- **THEN** the exported value is a `ZonedDateTime` in the system default timezone equivalent to the UTC input

### Requirement: Binary/file attribute conversion to filename
During payload transformation, `JxlExportUtil` SHALL convert binary (`FileType`) attribute values to their filename string representation.

#### Scenario: Exporting a payload with a binary file attribute
- **GIVEN** a `Payload` containing attribute `attachment` with a `FileType` value whose `fileName` is `"report.pdf"`, and the ASM type is a byte array type
- **WHEN** the payload list is transformed
- **THEN** the exported map entry for `attachment` contains the string `"report.pdf"`

### Requirement: Optional value unwrapping
During payload transformation, `JxlExportUtil` SHALL unwrap `Optional` values, using the contained value or null if empty.

#### Scenario: Exporting a payload with an Optional attribute
- **GIVEN** a `Payload` containing attribute `nickname` with value `Optional.of("Bob")`
- **WHEN** the payload list is transformed
- **THEN** the exported map entry for `nickname` contains `"Bob"`

### Requirement: Excel export to OutputStream with JXLS templating
`JxlExportUtil.createExcelExport()` SHALL create an XLSX workbook with a named sheet, generate a JXLS template with header row and data row placeholders, populate it with the transformed payload data, and write the result to the provided `OutputStream`.

#### Scenario: Exporting a non-empty list to OutputStream
- **GIVEN** an `AsmModel`, a sheet name `"Orders"`, a list of 3 `Payload` entries, target types, and attribute names
- **WHEN** `JxlExportUtil.createExcelExport()` is invoked
- **THEN** an XLSX workbook is written to the `OutputStream` containing a sheet named `"Orders"` with a header row and 3 data rows

#### Scenario: Exporting an empty list
- **GIVEN** an empty list of `Payload` entries
- **WHEN** `JxlExportUtil.createExcelExport()` is invoked
- **THEN** an XLSX workbook is written with a sheet containing only the header row and no data rows

### Requirement: Excel export to InputStream
`JxlExportUtil.createExcelExportToInputStream()` SHALL produce the same XLSX content as `createExcelExport()` but additionally return an `InputStream` for streaming the result, and optionally write to a provided `OutputStream`.

#### Scenario: Exporting to InputStream with simultaneous OutputStream
- **GIVEN** a non-empty payload list and a non-null `OutputStream`
- **WHEN** `JxlExportUtil.createExcelExportToInputStream()` is invoked
- **THEN** the XLSX content is written to both the `OutputStream` and the returned `InputStream`

### Requirement: Type-aware cell formatting in Excel templates
`JxlExportUtil.createTemplateSheet()` SHALL apply appropriate Excel cell styles based on ASM data types: `hh:mm:ss` for time types, `dd/mm/yyyy` for date types, `dd/mm/yyyy hh:mm:ss` for timestamp types, `#,##0.00` for decimal types, and `0` for integer types.

#### Scenario: Template sheet with mixed data types
- **GIVEN** attributes including a date, a timestamp, a decimal, and an integer
- **WHEN** the template sheet is created
- **THEN** the data row cells have date format `dd/mm/yyyy`, datetime format `dd/mm/yyyy hh:mm:ss`, decimal format `#,##0.00`, and integer format `0` respectively

### Requirement: Export interface compliance with AsmModel overloads
`JxlsExport` SHALL implement the `Export` interface, supporting the `AsmModel`-based `exportToOutputStream()` and `exportToInputStream()` overloads by delegating to `JxlExportUtil`. The `Class`-based overloads SHALL throw `UnsupportedOperationException`.

#### Scenario: AsmModel-based export to OutputStream
- **GIVEN** a `JxlsExport` instance, an `AsmModel`, and a fully qualified entity name
- **WHEN** `exportToOutputStream(output, type, list, attributes, asmModel, fqName, locale)` is invoked
- **THEN** the call is delegated to `JxlExportUtil.createExcelExport()` with the sheet name derived from the simple class name portion of `fqName`

#### Scenario: Class-based export throws UnsupportedOperationException
- **GIVEN** a `JxlsExport` instance
- **WHEN** `exportToOutputStream(output, type, list, attributes, clazz, locale)` is invoked
- **THEN** an `UnsupportedOperationException` is thrown
