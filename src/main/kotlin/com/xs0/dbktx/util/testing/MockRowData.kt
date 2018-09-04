package com.xs0.dbktx.util.testing

import com.xs0.asyncdb.common.RowData
import java.util.AbstractList

class MockRowData(private val elements: Array<Any?>, private val colMapping: Map<String, Int>, private val rowNum: Int) : AbstractList<Any?>(), RowData {
    override val size: Int
        get() = elements.size

    override fun get(index: Int): Any? {
        return elements[index]
    }

    override fun get(columnName: String?): Any? {
        return elements[colMapping[columnName] ?: -1]
    }

    override fun getRowNumber(): Int {
        return rowNum
    }
}