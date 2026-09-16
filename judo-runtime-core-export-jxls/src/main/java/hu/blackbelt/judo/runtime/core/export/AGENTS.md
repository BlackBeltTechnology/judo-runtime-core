# AGENTS.md — `judo-runtime-core-export-jxls/src/main/java/hu/blackbelt/judo/runtime/core/export`

jxls/Apache POI backed XLSX export of DAO `Payload` lists, bound to ASM attribute types.

| File | Purpose |
| --- | --- |
| `JxlExportUtil.java` | Renders `Payload` lists to XSSF workbooks via jxls. Exports `getAttributesFromModel(AsmModel, String)`, `createExcelExport(...)`, `createExcelExportToInputStream(...)`. Writes attribute header row 0 plus `${context.<attr>}` template row 1 styled by ASM type. Converts enum ints to literals, byte-arrays to `FileType` name, `Double`/`Float` to `BigDecimal`, UTC `LocalDateTime` to system zone. |
| `JxlsExport.java` | Implements `Export` over `JxlExportUtil`, using the FQ name segment after the last `.` as sheet name. Only the `AsmModel`-carrying `exportToOutputStream` / `exportToInputStream` overloads work; the `Class`-based overloads throw `UnsupportedOperationException`. Column set comes from `getAttributesFromModel`, order from the caller's `attributes` list. |
