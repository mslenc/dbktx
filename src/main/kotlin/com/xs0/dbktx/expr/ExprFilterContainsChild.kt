package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.ManyToOneInfo
import com.xs0.dbktx.util.Sql

class ExprFilterContainsChild<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>(
        private val parentTable: TableInQuery<FROM>,
        private val info: ManyToOneInfo<TO, *, FROM, *>,
        private val filter: ExprBoolean,
        private val childTable: TableInQuery<TO>,
        private val negated: Boolean = false) : ExprBoolean {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        val mappings = info.columnMappings
        val n = mappings.size

        sql.expr(topLevel) {
            paren(n > 1) {
                tuple(mappings) {
                    sql(it.columnTo.bindForSelect(parentTable), false)
                }
            }
            +(if (negated) " NOT IN " else " IN ")
            +"(SELECT "
            tuple(mappings) {
                +it.columnFrom.bindForSelect(childTable)
            }
            FROM(info.manyTable, childTable.tableAlias)
            WHERE(filter)
            +")"
        }
    }

    override fun not(): ExprBoolean {
        return ExprFilterContainsChild(parentTable, info, filter, childTable, !negated)
    }
}
