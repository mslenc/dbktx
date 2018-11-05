package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.schemas.test2.CompEntry
import com.github.mslenc.dbktx.schemas.test2.CompResult
import com.github.mslenc.dbktx.schemas.test2.Weight

fun testApi(db: DbConn) {
    Weight.aggregateQuery(db) {
        +Weight.NAME

        innerJoin(Weight.ENTRIES_SET) {
            filter {
                CompEntry.ID_COUNTRY eq 123
            }

            +CompEntry.ID_PERSON.avg()
            +CompEntry.ID_PERSON.min()

            innerJoin(CompEntry.REF_RESULT) {
                +CompResult.PLACE.avg()
            }
        }
    }
}