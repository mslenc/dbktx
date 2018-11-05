package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.asyncdb.DbValue

class AggregateRow(private val rowData: DbRow) {
    operator fun get(indexInRow: Int): DbValue {
        return rowData.getValue(indexInRow)
    }
}