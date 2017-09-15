package com.xs0.dbktx.expr

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.ManyToOneInfo
import com.xs0.dbktx.util.Sql

class ExprFilterContainsChild<FROM : DbEntity<FROM, FID>, FID: Any, TO : DbEntity<TO, TID>, TID: Any>(
        private val info: ManyToOneInfo<TO, TID, FROM, FID>,
        private val filter: ExprBoolean<TO>) : ExprBoolean<FROM> {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        val mappings = info.columnMappings
        val n = mappings.size

        sql.expr(topLevel) {
            paren(n > 1) {
                tuple(mappings) {
                    +it.columnTo
                }
            }
            +" IN (SELECT "
            tuple(mappings) {
                +it.columnFrom
            }
            FROM(info.manyTable)
            WHERE(filter)
            +")"
        }
    }
}
