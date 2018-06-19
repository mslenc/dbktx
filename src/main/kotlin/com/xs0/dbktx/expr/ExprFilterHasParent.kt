package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.ManyToOneInfo
import com.xs0.dbktx.util.Sql

class ExprFilterHasParent<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>(
        private val info: ManyToOneInfo<FROM, *, TO, *>,
        private val filter: ExprBoolean,
        private val srcTable: TableInQuery<FROM>,
        private val dstTable: TableInQuery<TO>,
        private val negated: Boolean = false) : ExprBoolean {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        val mappings = info.columnMappings
        val n = mappings.size

        sql.expr(topLevel) {
            paren(n > 1) {
                tuple(info.columnMappings) {
                    columnForSelect(srcTable, it.columnFrom)
                }
            }
            +(if (negated) " NOT IN " else " IN ")
            +"(SELECT "
            tuple(info.columnMappings) {
                columnForSelect(dstTable, it.columnTo)
            }
            FROM(dstTable)
            WHERE(filter)
            +")"
        }
    }

    override fun not(): ExprBoolean {
        return ExprFilterHasParent(info, filter, srcTable, dstTable, !negated)
    }
}
