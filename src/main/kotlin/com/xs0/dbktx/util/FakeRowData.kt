package com.xs0.dbktx.util

import com.github.mslenc.asyncdb.common.RowData
import com.xs0.dbktx.schema.Column
import com.xs0.dbktx.schema.DbTable

class FakeRowData : RowData {
    private val valueByIndex = HashMap<Int, Any?>()
    private val valueByName = HashMap<String, Any?>()

    fun <T: Any> put(column: Column<*, T>, value: T?) {
        valueByIndex[column.indexInRow] = value
        valueByName[column.fieldName] = value
    }

    fun <T: Any> insertDummyValue(column: Column<*, T>) {
        put(column, column.sqlType.dummyValue)
    }

    fun <T: Any> insertJsonValue(column: Column<*, T>, value: Any) {
        put(column, column.sqlType.decodeFromJson(value))
    }

    override fun get(columnIndex: Int): Any? {
        return valueByIndex[columnIndex]
    }

    override fun get(columnName: String): Any? {
        return valueByName[columnName]
    }

    override fun getRowNumber(): Int {
        return 0
    }

    companion object {
        fun of(dbTable: DbTable<*, *>, vararg values: Any?): FakeRowData {
            if (values.size != dbTable.numColumns)
                throw IllegalArgumentException("Number of values (${values.size}) does not match number of columns in table (${dbTable.numColumns})")

            val result = FakeRowData()

            for (index in values.indices) {
                result.putConverted(dbTable.columns[index], values[index])
            }

            return result
        }

        private fun <T: Any> FakeRowData.putConverted(column: Column<*, T>, value: Any?) {
            if (value != null) {
                put(column, column.sqlType.parseRowDataValue(value))
            }
        }
    }
}
