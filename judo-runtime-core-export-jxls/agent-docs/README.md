# JUDO JXLS Export - Agent Documentation

## Overview

This module provides Excel export functionality for JUDO applications using JXLS and Apache POI. It implements the `Export` interface from `judo-runtime-core-dispatcher` to export `Payload` data to Excel format (.xlsx).

## Key Components

### JxlsExport

Main export class implementing the `Export` interface:

```java
public class JxlsExport implements Export {
    // Export Payload list to Excel via OutputStream
    void exportToOutputStream(OutputStream output, String type, List<Payload> list, 
                              List<String> attributes, AsmModel asmModel, 
                              String fqName, Locale locale);
    
    // Export Payload list and return InputStream
    InputStream exportToInputStream(String type, List<Payload> list, 
                                    List<String> attributes, OutputStream output,
                                    AsmModel asmModel, String fqName, Locale locale);
}
```

### JxlExportUtil

Utility class for creating Excel exports with automatic:
- Type conversion (enums, dates, decimals, etc.)
- Column header generation from attribute names
- Cell formatting based on data types (date, time, decimal, integer)
- Template sheet creation with JXLS expressions

## Supported Data Types

| Type | Excel Format |
|------|--------------|
| Timestamp | dd/mm/yyyy hh:mm:ss |
| Date | dd/mm/yyyy |
| Time | hh:mm:ss |
| Decimal/Double/Float | #,##0.00 |
| Integer | 0 |
| Enum | Literal value |
| FileType | File name only |

## Usage Example

```java
// Create export instance
JxlsExport export = new JxlsExport();

// Export to OutputStream
try (OutputStream out = new FileOutputStream("export.xlsx")) {
    export.exportToOutputStream(
        out,
        "xlsx",
        payloadList,           // List<Payload> data
        attributeNames,        // List<String> columns to export
        asmModel,              // AsmModel for type information
        "com.example.Entity",  // Fully qualified entity name
        Locale.getDefault()
    );
}

// Or get as InputStream
InputStream excelStream = export.exportToInputStream(
    "xlsx", payloadList, attributeNames, null, asmModel, fqName, locale
);
```

## Dependencies

- **JXLS** - Excel template engine
- **Apache POI** - Excel file manipulation
- **judo-meta-asm** - ASM model for type information

## Files in This Package

| File | Content |
|------|---------|
| `README.md` | This file - overview and usage documentation |
