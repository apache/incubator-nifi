/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.excel;

import com.github.pjfanning.xlsx.StreamingReader;
import com.github.pjfanning.xlsx.exceptions.ExcelRuntimeException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.excel.ProtectionType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.poi.common.Duplicatable;
import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.formula.FormulaShifter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellCopyContext;
import org.apache.poi.ss.usermodel.CellCopyPolicy;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.helpers.XSSFRowShifter;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;

@SideEffectFree
@SupportsBatching
@Tags({"split", "text"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor splits a multi sheet Microsoft Excel spreadsheet into multiple Microsoft Excel spreadsheets where each sheet from the original" +
        " file is converted to an individual spreadsheet in its own flow file. Currently this processor is only capable of processing .xlsx" +
        " (XSSF 2007 OOXML file format) Excel documents and not older .xls (HSSF '97(-2007) file format) documents." +
        " Please note all original cell styles are dropped and formulas are removed leaving only the calculated values." +
        " Even a single sheet Microsoft Excel spreadsheet is converted to its own flow file with all the original cell styles dropped and formulas removed."
)
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier", description = "All split Excel FlowFiles produced from the same parent Excel FlowFile will have the same randomly generated UUID added" +
                " for this attribute"),
        @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split Excel FlowFiles that were created from a single parent Excel FlowFile"),
        @WritesAttribute(attribute = "fragment.count", description = "The number of split Excel FlowFiles generated from the parent Excel FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename", description = "The filename of the parent Excel FlowFile"),
        @WritesAttribute(attribute = SplitExcel.SHEET_NAME, description = "The name of the Excel sheet from the original spreadsheet."),
        @WritesAttribute(attribute = SplitExcel.TOTAL_ROWS, description = "The number of rows in the Excel sheet from the original spreadsheet.")})
public class SplitExcel extends AbstractProcessor {
    public static final String SHEET_NAME = "sheetname";
    public static final String TOTAL_ROWS = "total.rows";

    public static final PropertyDescriptor PROTECTION_TYPE = new PropertyDescriptor.Builder()
            .name("Protection Type")
            .description("Specifies whether an Excel spreadsheet is protected by a password or not.")
            .required(true)
            .allowableValues(ProtectionType.class)
            .defaultValue(ProtectionType.UNPROTECTED)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password for a password protected Excel spreadsheet")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROTECTION_TYPE, ProtectionType.PASSWORD)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, the unchanged FlowFile will be routed to this relationship.")
            .build();

    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("The individual Excel 'segments' of the original Excel FlowFile will be routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PROTECTION_TYPE,
            PASSWORD
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_FAILURE,
            REL_SPLIT
    );

    private static final CellCopyPolicy CELL_COPY_POLICY = new CellCopyPolicy.Builder()
            .cellFormula(false) // NOTE: setting to false allows for copying the evaluated formula value.
            .cellStyle(CellCopyPolicy.DEFAULT_COPY_CELL_STYLE_POLICY)
            .cellValue(CellCopyPolicy.DEFAULT_COPY_CELL_VALUE_POLICY)
            .condenseRows(CellCopyPolicy.DEFAULT_CONDENSE_ROWS_POLICY)
            .copyHyperlink(CellCopyPolicy.DEFAULT_COPY_HYPERLINK_POLICY)
            .mergeHyperlink(CellCopyPolicy.DEFAULT_MERGE_HYPERLINK_POLICY)
            .mergedRegions(CellCopyPolicy.DEFAULT_COPY_MERGED_REGIONS_POLICY)
            .rowHeight(CellCopyPolicy.DEFAULT_COPY_ROW_HEIGHT_POLICY)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }

        final String password = context.getProperty(PASSWORD).getValue();
        final List<WorkbookSplit> workbookSplits = new ArrayList<>();

        try {
            session.read(originalFlowFile, in -> {

                final Workbook originalWorkbook = StreamingReader.builder()
                        .rowCacheSize(100)
                        .bufferSize(4096)
                        .password(password)
                        .setReadHyperlinks(true) // NOTE: Needed for copying rows.
                        .setReadSharedFormulas(true) // NOTE: If not set to true, then data with shared formulas fail.
                        .open(in);

                int index = 0;
                for (final Sheet originalSheet : originalWorkbook) {
                    final String originalSheetName = originalSheet.getSheetName();
                    try (XSSFWorkbook newWorkbook = new XSSFWorkbook()) {
                        XSSFSheet newSheet = newWorkbook.createSheet(originalSheetName);
                        List<Row> originalRows = new ArrayList<>();
                        for (Row originalRow : originalSheet) {
                            originalRows.add(originalRow);
                        }

                        if (!originalRows.isEmpty()) {
                            copyRows(newSheet, originalRows, originalSheet.getFirstRowNum());
                        }

                        FlowFile newFlowFile = session.create(originalFlowFile);
                        try (final OutputStream out = session.write(newFlowFile)) {
                            newWorkbook.write(out);
                            workbookSplits.add(new WorkbookSplit(index, newFlowFile, originalSheetName, originalRows.size()));
                        }
                    }

                    index++;
                }
            });
        } catch (ExcelRuntimeException | IllegalStateException | ProcessException e) {
            getLogger().error("Failed to split {}", originalFlowFile, e);
            session.remove(workbookSplits.stream()
                    .map(WorkbookSplit::content)
                    .toList());
            workbookSplits.clear();
            session.transfer(originalFlowFile, REL_FAILURE);
            return;
        }

        final String fragmentId = UUID.randomUUID().toString();
        final String originalFileName = originalFlowFile.getAttribute(CoreAttributes.FILENAME.key());
        final int extensionIndex = originalFileName.lastIndexOf(".");
        String originalFileNameWithoutExtension = originalFileName;
        String originalFileNameExtension = "";

        if (extensionIndex > -1) {
            originalFileNameWithoutExtension = originalFileName.substring(0, extensionIndex);
            originalFileNameExtension = originalFileName.substring(extensionIndex);
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(FRAGMENT_COUNT.key(), String.valueOf(workbookSplits.size()));
        attributes.put(FRAGMENT_ID.key(), fragmentId);
        attributes.put(SEGMENT_ORIGINAL_FILENAME.key(), originalFileName);

        for (WorkbookSplit split : workbookSplits) {
            attributes.put(CoreAttributes.FILENAME.key(), String.format("%s-%s%s", originalFileNameWithoutExtension, split.index(), originalFileNameExtension));
            attributes.put(FRAGMENT_INDEX.key(), Integer.toString(split.index()));
            attributes.put(SHEET_NAME, split.sheetName());
            attributes.put(TOTAL_ROWS, Integer.toString(split.numRows()));
            session.putAllAttributes(split.content(), attributes);
        }

        session.transfer(originalFlowFile, REL_ORIGINAL);
        final List<FlowFile> flowFileSplits = workbookSplits.stream()
                .map(WorkbookSplit::content)
                .toList();

        session.transfer(flowFileSplits, REL_SPLIT);
    }

    private record WorkbookSplit(int index, FlowFile content, String sheetName, int numRows) {
    }

    /*org.apache.poi.xssf.usermodel.XSSFSheet copyRows method refactored with Intellij suggestions
      and instantiates a CellCopyContext to avoid exceeding the maximum number of cell styles (64000) in a .xlsx Workbook.
      This instance of CellCopyContext is eventually passed to copyCell.
     */
    static void copyRows(XSSFSheet destSheet, List<? extends Row> srcRows, int destStartRow) {
        if (srcRows != null && !srcRows.isEmpty()) {
            Row srcStartRow = srcRows.getFirst();
            Row srcEndRow = srcRows.getLast();

            if (srcStartRow == null) {
                throw new IllegalArgumentException("copyRows: First row cannot be null");
            } else {
                int srcStartRowNum = srcStartRow.getRowNum();
                int srcEndRowNum = srcEndRow.getRowNum();

                for (int index = 1; index < srcRows.size(); ++index) {
                    Row curRow = srcRows.get(index);
                    if (curRow == null) {
                        throw new IllegalArgumentException("srcRows may not contain null rows. Found null row at index " + index + ".");
                    }

                    if (srcStartRow.getSheet().getWorkbook() != curRow.getSheet().getWorkbook()) {
                        throw new IllegalArgumentException("All rows in srcRows must belong to the same sheet in the same workbook. Expected all rows from same workbook ("
                                + srcStartRow.getSheet().getWorkbook() + "). Got srcRows[" + index + "] from different workbook (" + curRow.getSheet().getWorkbook() + ").");
                    }

                    if (srcStartRow.getSheet() != curRow.getSheet()) {
                        throw new IllegalArgumentException("All rows in srcRows must belong to the same sheet. Expected all rows from "
                                + srcStartRow.getSheet().getSheetName() + ". Got srcRows[" + index + "] from " + curRow.getSheet().getSheetName());
                    }
                }

                CellCopyPolicy options = new CellCopyPolicy(SplitExcel.CELL_COPY_POLICY);
                options.setCopyMergedRegions(false);
                int r = destStartRow;
                CellCopyContext context = new CellCopyContext();

                for (Row srcRow : srcRows) {
                    int destRowNum;
                    if (SplitExcel.CELL_COPY_POLICY.isCondenseRows()) {
                        destRowNum = r++;
                    } else {
                        int shift = srcRow.getRowNum() - srcStartRowNum;
                        destRowNum = destStartRow + shift;
                    }

                    XSSFRow destRow = destSheet.createRow(destRowNum);
                    copyRowFrom(destRow, srcRow, options, context);
                }

                if (SplitExcel.CELL_COPY_POLICY.isCopyMergedRegions()) {
                    int shift = destStartRow - srcStartRowNum;

                    for (CellRangeAddress srcRegion : srcStartRow.getSheet().getMergedRegions()) {
                        if (srcStartRowNum <= srcRegion.getFirstRow() && srcRegion.getLastRow() <= srcEndRowNum) {
                            CellRangeAddress destRegion = srcRegion.copy();
                            destRegion.setFirstRow(destRegion.getFirstRow() + shift);
                            destRegion.setLastRow(destRegion.getLastRow() + shift);
                            destSheet.addMergedRegion(destRegion);
                        }
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("No rows to copy");
        }
    }

    /*org.apache.poi.xssf.usermodel.XSSFRow copyRowFrom method refactored with Intellij suggestions*/
    static void copyRowFrom(Row destRow, Row srcRow, CellCopyPolicy policy, CellCopyContext context) {
        if (srcRow == null) {
            for (Cell cell : destRow) {
                copyCell(null, cell, policy, context);
            }

            if (policy.isCopyMergedRegions()) {
                int destRowNum = destRow.getRowNum();
                int index = 0;
                Set<Integer> indices = new HashSet<>();

                for (Iterator<CellRangeAddress> cellRangeAddressIterator = destRow.getSheet().getMergedRegions().iterator(); cellRangeAddressIterator.hasNext(); ++index) {
                    CellRangeAddress destRegion = cellRangeAddressIterator.next();
                    if (destRowNum == destRegion.getFirstRow() && destRowNum == destRegion.getLastRow()) {
                        indices.add(index);
                    }
                }

                destRow.getSheet().removeMergedRegions(indices);
            }

            if (policy.isCopyRowHeight()) {
                destRow.setHeight((short) -1);
            }
        } else {
            for (Cell srcCell : srcRow) {
                Cell destCell = destRow.createCell(srcCell.getColumnIndex());
                copyCell(srcCell, destCell, policy, context);
            }

            int destRowNum = destRow.getSheet().getWorkbook().getSheetIndex(destRow.getSheet());
            String sheetName = destRow.getSheet().getWorkbook().getSheetName(destRowNum);
            int srcRowNum = srcRow.getRowNum();
            destRowNum = destRow.getRowNum();
            int rowDifference = destRowNum - srcRowNum;
            FormulaShifter formulaShifter = FormulaShifter.createForRowCopy(destRowNum, sheetName, srcRowNum, srcRowNum, rowDifference, SpreadsheetVersion.EXCEL2007);
            XSSFRowShifter rowShifter = new XSSFRowShifter((XSSFSheet) destRow.getSheet());
            rowShifter.updateRowFormulas((XSSFRow) destRow, formulaShifter);

            if (policy.isCopyMergedRegions()) {
                for (CellRangeAddress srcRegion : srcRow.getSheet().getMergedRegions()) {
                    if (srcRowNum == srcRegion.getFirstRow() && srcRowNum == srcRegion.getLastRow()) {
                        CellRangeAddress destRegion = srcRegion.copy();
                        destRegion.setFirstRow(destRowNum);
                        destRegion.setLastRow(destRowNum);
                        destRow.getSheet().addMergedRegion(destRegion);
                    }
                }
            }

            if (policy.isCopyRowHeight()) {
                destRow.setHeight(srcRow.getHeight());
            }
        }
    }

    /*Taken from org.apache.poi.ss.util.CellUtil refactored with Intellij suggestions and to only
      use the numeric setter for numeric values even though the cell may represent a date, otherwise
      dates which represent times are not set correctly.
    */
    static void copyCell(Cell srcCell, Cell destCell, CellCopyPolicy policy, CellCopyContext context) {
        if (policy.isCopyCellValue()) {
            if (srcCell != null) {
                CellType copyCellType = srcCell.getCellType();
                if (copyCellType == CellType.FORMULA && !policy.isCopyCellFormula()) {
                    copyCellType = srcCell.getCachedFormulaResultType();
                }

                switch (copyCellType) {
                    case NUMERIC ->
                        /*NOTE: The original CellUtil code determined whether the cell was date formatted and if so
                          used the setter method which takes a java.util.Date. That proved problematic for cells which
                          had formatted times with an illegal Excel date of 12/31/1899. To bypass that issue, only
                          the setter for the numeric value is used.*/
                        destCell.setCellValue(srcCell.getNumericCellValue());
                    case STRING -> destCell.setCellValue(srcCell.getRichStringCellValue());
                    case FORMULA -> destCell.setCellFormula(srcCell.getCellFormula());
                    case BLANK -> destCell.setBlank();
                    case BOOLEAN -> destCell.setCellValue(srcCell.getBooleanCellValue());
                    case ERROR -> destCell.setCellErrorValue(srcCell.getErrorCellValue());
                    default -> throw new IllegalArgumentException("Invalid cell type " + srcCell.getCellType());
                }
            } else {
                destCell.setBlank();
            }
        }

        if (policy.isCopyCellStyle() && srcCell != null) {
            if (srcCell.getSheet() != null && destCell.getSheet() != null && destCell.getSheet().getWorkbook() == srcCell.getSheet().getWorkbook()) {
                destCell.setCellStyle(srcCell.getCellStyle());
            } else {
                CellStyle srcStyle = srcCell.getCellStyle();
                CellStyle destStyle = context == null ? null : context.getMappedStyle(srcStyle);
                if (destStyle == null) {
                    destStyle = destCell.getSheet().getWorkbook().createCellStyle();
                    destStyle.cloneStyleFrom(srcStyle);
                    if (context != null) {
                        context.putMappedStyle(srcStyle, destStyle);
                    }
                }

                destCell.setCellStyle(destStyle);
            }
        }

        Hyperlink srcHyperlink = srcCell == null ? null : srcCell.getHyperlink();
        Hyperlink newHyperlink;
        if (policy.isMergeHyperlink()) {
            if (srcHyperlink != null) {
                if (!(srcHyperlink instanceof Duplicatable)) {
                    throw new IllegalStateException("srcCell hyperlink is not an instance of Duplicatable");
                }

                newHyperlink = (Hyperlink) ((Duplicatable) srcHyperlink).copy();
                destCell.setHyperlink(newHyperlink);
            }
        } else if (policy.isCopyHyperlink()) {
            if (srcHyperlink == null) {
                destCell.setHyperlink(null);
            } else {
                if (!(srcHyperlink instanceof Duplicatable)) {
                    throw new IllegalStateException("srcCell hyperlink is not an instance of Duplicatable");
                }

                newHyperlink = (Hyperlink) ((Duplicatable) srcHyperlink).copy();
                destCell.setHyperlink(newHyperlink);
            }
        }
    }
}
