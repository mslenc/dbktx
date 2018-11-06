package com.github.mslenc.dbktx.util

import com.github.mslenc.asyncdb.DbColumns
import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueNull

/**
 * A list which pulls data from some entity via some arbitrary mapping.
 */
class RemappingList<T>(val mapping: Map<Int, (T)->DbValue?>, val entity: T): DbRow {
    override fun getValue(columnIndex: Int): DbValue {
        val mapper = mapping[columnIndex] ?: return DbValueNull.instance()
        return mapper(entity) ?: DbValueNull.instance()
    }

    override fun get(columnName: String?): Any {
        TODO("not implemented")
    }

    override fun getRowIndex(): Int {
        return 0
    }

    override fun getColumns(): DbColumns {
        TODO("not implemented")
    }

    override fun getValue(columnName: String?): DbValue {
        TODO("not implemented")
    }
}