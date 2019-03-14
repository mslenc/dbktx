package com.github.mslenc.dbktx.util

import com.github.mslenc.asyncdb.DbColumns
import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueNull
import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.schema.DbTable

class FakeRowData : DbRow {
    private val valueByIndex = HashMap<Int, DbValue>()
    private val valueByName = HashMap<String, DbValue>()

    fun <T: Any> put(column: Column<*, T>, value: T?) {
        if (value != null) {
            val dbValue = column.sqlType.makeDbValue(value)
            valueByIndex[column.indexInRow] = dbValue
            valueByName[column.fieldName] = dbValue
        }
    }

    override fun getValue(columnIndex: Int): DbValue {
        return valueByIndex[columnIndex] ?: DbValueNull.instance()
    }

    override fun getValue(columnName: String?): DbValue {
        return valueByName[columnName] ?: DbValueNull.instance()
    }

    fun <T: Any> insertDummyValue(column: Column<*, T>) {
        put(column, column.sqlType.zeroValue)
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

    override fun getRowIndex(): Int {
        return 0
    }

    override fun getColumns(): DbColumns {
        TODO("not implemented")
    }

    companion object {
        fun of(dbTable: DbTable<*, *>, vararg values: Any?): FakeRowData {
            if (values.size != dbTable.numColumns)
                throw IllegalArgumentException("Number of values (${values.size}) does not match number of columns in table (${dbTable.numColumns})")

            val result = FakeRowData()

            for (index in values.indices) {
                val column = dbTable.columns[index]
                result.putConverted(column, values[index])
            }

            return result
        }

        private fun <T: Any> FakeRowData.putConverted(column: Column<*, T>, value: Any?) {
            if (value != null) {
                @Suppress("UNCHECKED_CAST")
                put(column, value as T)
            }
        }
    }
}
