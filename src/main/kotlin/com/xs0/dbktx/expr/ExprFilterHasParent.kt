package com.xs0.dbktx.expr

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.ManyToOneInfo
import com.xs0.dbktx.util.Sql

class ExprFilterHasParent<FROM : DbEntity<FROM, FID>, FID : Any, TO : DbEntity<TO, TID>, TID : Any>(
        private val info: ManyToOneInfo<FROM, FID, TO, TID>,
        private val filter: ExprBoolean<TO>) : ExprBoolean<FROM> {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        val mappings = info.columnMappings
        val n = mappings.size

        sql.expr(topLevel) {
            paren(n > 1) {
                tuple(info.columnMappings) {
                    +it.columnFrom
                }
            }
            +" IN (SELECT "
            tuple(info.columnMappings) {
                +it.columnTo
            }
            FROM(info.oneTable)
            WHERE(filter)
            +")"
        }
    }
}
