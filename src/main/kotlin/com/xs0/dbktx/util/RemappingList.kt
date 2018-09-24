package com.xs0.dbktx.util

import com.github.mslenc.asyncdb.common.RowData

/**
 * A list which pulls data from some entity via some arbitrary mapping.
 */
class RemappingList<T>(val mapping: Map<Int, (T)->Any?>, val entity: T): RowData {
    override fun get(index: Int): Any? {
        val mapper = mapping[index] ?: return null
        return mapper(entity)
    }

    override fun get(columnName: String?): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getRowNumber(): Int {
        return 0
    }
}