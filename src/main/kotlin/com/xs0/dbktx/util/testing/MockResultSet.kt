package com.xs0.dbktx.util.testing

import com.xs0.asyncdb.common.ResultSet
import com.xs0.asyncdb.common.RowData
import java.util.AbstractList

class MockResultSet(private val columnNames: Array<String>) : AbstractList<RowData>(), ResultSet {
    private val rows = ArrayList<MockRowData>()

    private val columnIndex: Map<String, Int> = with(columnNames) {
        val res = HashMap<String, Int>()
        for (i in columnNames.indices)
            res[columnNames[i]] = i
        res
    }

    override fun get(index: Int): RowData = rows[index]

    override val size: Int
        get() = rows.size

    override fun getColumnNames(): List<String> {
        return columnNames.toList()
    }

    fun addRow(values: Array<Any?>) {
        rows.add(MockRowData(values, columnIndex, rows.size))
    }
}