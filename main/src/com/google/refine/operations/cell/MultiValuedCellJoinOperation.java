/*

Copyright 2010, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package com.google.refine.operations.cell;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.Validate;

import com.google.refine.expr.ExpressionUtils;
import com.google.refine.history.HistoryEntry;
import com.google.refine.model.AbstractOperation;
import com.google.refine.model.Cell;
import com.google.refine.model.Column;
import com.google.refine.model.Project;
import com.google.refine.model.Row;
import com.google.refine.model.changes.MassRowChange;
import com.google.refine.operations.OperationDescription;

public class MultiValuedCellJoinOperation extends AbstractOperation {

    private final Project project;
    private final String _columnName;
    private final String _keyColumnName;
    private final String _separator;

    @JsonCreator
    public MultiValuedCellJoinOperation(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("keyColumnName") String keyColumnName,
            @JsonProperty("separator") String separator,
            @JsonProperty("project") Project project) {
        this.project = project;
        _columnName = columnName;
        _keyColumnName = keyColumnName;
        _separator = separator;
    }
    
    // constructor with the project value as null
    public MultiValuedCellJoinOperation(String columnName, String keyColumnName, String separator) {
        this(columnName, keyColumnName, separator, null);
    }

    @Override
    public void validate() {
        Validate.notNull(_columnName, "Missing column name");
        Validate.notNull(_keyColumnName, "Missing key column name");
        Validate.notNull(_separator, "Missing separator");
    }

    @JsonProperty("columnName")
    public String getColumnName() {
        return _columnName;
    }

    @JsonProperty("keyColumnName")
    public String getKeyColumnName() {
        return _keyColumnName;
    }

    @JsonProperty("separator")
    public String getSeparator() {
        return _separator;
    }

    @JsonProperty("project")
    public Project getProject() {
        return project;
    }

    @Override
    protected String getBriefDescription(Project project) {
        return OperationDescription.cell_multivalued_cell_join_brief(_columnName);
    }

    public HistoryEntry createHistoryEntry(long historyEntryID) throws Exception {
        Column column = project.columnModel.getColumnByName(_columnName);
        if (column == null) {
            throw new Exception("No column named " + _columnName);
        }
        int cellIndex = column.getCellIndex();

        Column keyColumn = project.columnModel.getColumnByName(_keyColumnName);
        if (keyColumn == null) {
            throw new Exception("No key column named " + _keyColumnName);
        }
        int keyCellIndex = keyColumn.getCellIndex();

        List<Row> newRows = new ArrayList<Row>();

        int oldRowCount = project.rows.size();
        for (int r = 0; r < oldRowCount; r++) {
            Row oldRow = project.rows.get(r);

            if (oldRow.isCellBlank(keyCellIndex)) {
                newRows.add(oldRow.dup());
                continue;
            }

            int r2 = r + 1;
            while (r2 < oldRowCount && project.rows.get(r2).isCellBlank(keyCellIndex)) {
                r2++;
            }

            if (r2 == r + 1) {
                newRows.add(oldRow.dup());
                continue;
            }

            StringBuffer sb = new StringBuffer();
            for (int r3 = r; r3 < r2; r3++) {
                Object value = project.rows.get(r3).getCellValue(cellIndex);
                if (ExpressionUtils.isNonBlankData(value)) {
                    if (sb.length() > 0) {
                        sb.append(_separator);
                    }
                    sb.append(value.toString());
                }
            }

            for (int r3 = r; r3 < r2; r3++) {
                Row newRow = project.rows.get(r3).dup();
                if (r3 == r) {
                    newRow.setCell(cellIndex, new Cell(sb.toString(), null));
                } else {
                    newRow.setCell(cellIndex, null);
                }

                if (!newRow.isEmpty()) {
                    newRows.add(newRow);
                }
            }

            r = r2 - 1; // r will be incremented by the for loop anyway
        }

        return new HistoryEntry(
                historyEntryID,
                project,
                getBriefDescription(null),
                this,
                new MassRowChange(newRows));
    }

    @Override
    protected HistoryEntry createHistoryEntry(Project project, long historyEntryID) throws Exception {
        // use the project instance if provided, otherwise use the stored project
        Project usedProject = project != null ? project : this.project;
        Column column = usedProject.columnModel.getColumnByName(_columnName);
        if (column == null) {
            throw new Exception("No column named " + _columnName);
        }
        int cellIndex = column.getCellIndex();
        Column keyColumn = usedProject.columnModel.getColumnByName(_keyColumnName);
        if (keyColumn == null) {
            throw new Exception("No key column named " + _keyColumnName);
        }
        int keyCellIndex = keyColumn.getCellIndex();
        List<Row> newRows = generateNewRows(usedProject, cellIndex, keyCellIndex);
        return new HistoryEntry(historyEntryID, usedProject, getBriefDescription(usedProject), this, new MassRowChange(newRows));
    }

    public List<Row> generateNewRows(Project project, int cellIndex, int keyCellIndex) {
        List<Row> newRows = new ArrayList<>();
        int oldRowCount = project.rows.size();
        for (int r = 0; r < oldRowCount; r++) {
            Row oldRow = project.rows.get(r);
            if (oldRow.isCellBlank(keyCellIndex)) {
                newRows.add(oldRow.dup());
                continue;
            }
            int r2 = r + 1;
            while (r2 < oldRowCount && project.rows.get(r2).isCellBlank(keyCellIndex)) {
                r2++;
            }
            if (r2 == r + 1) {
                newRows.add(oldRow.dup());
                continue;
            }
            StringBuffer sb = new StringBuffer();
            for (int r3 = r; r3 < r2; r3++) {
                Object value = project.rows.get(r3).getCellValue(cellIndex);
                if (ExpressionUtils.isNonBlankData(value)) {
                    if (sb.length() > 0) {
                        sb.append(_separator);
                    }
                    sb.append(value.toString());
                }
            }
            for (int r3 = r; r3 < r2; r3++) {
                Row newRow = project.rows.get(r3).dup();
                if (r3 == r) {
                    newRow.setCell(cellIndex, new Cell(sb.toString(), null));
                } else {
                    newRow.setCell(cellIndex, null);
                }
                if (!newRow.isEmpty()) {
                    newRows.add(newRow);
                }
            }
            r = r2 - 1; // r will be incremented by the for loop anyway
        }

        return newRows;
    }

}
