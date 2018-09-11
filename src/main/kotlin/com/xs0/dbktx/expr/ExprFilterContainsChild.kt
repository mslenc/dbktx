package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.ManyToOneInfo
import com.xs0.dbktx.util.Sql

class ExprFilterContainsChild<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>(
        private val parentTable: TableInQuery<FROM>,
        private val info: ManyToOneInfo<TO, FROM, *>,
        private val filter: ExprBoolean?,
        private val childTable: TableInQuery<TO>,
        private val negated: Boolean = false) : ExprBoolean {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        val mappings = info.columnMappings
        val n = mappings.size

        // for now, we expect never to get here, if we don't have actual columns (instead, there are CONSTANT or PARAMETER mappings)
        // TODO: we could actually do it - CONSTANTs would become simple restrictions on this table, and PARAMETERS could be ignored,
        // but it's unclear if it'd be actually useful anywhere, so postponing for now

        sql.expr(topLevel) {
            paren(n > 1) {
                tuple(mappings) {
                    sql(it.bindColumnTo(parentTable), false)
                }
            }
            +(if (negated) " NOT IN " else " IN ")
            +"(SELECT "
            tuple(mappings) {
                +it.bindColumnFrom(childTable)
            }
            FROM(info.manyTable, childTable.tableAlias)
            WHERE(filter)
            +")"
        }
    }

    override fun not(): ExprBoolean {
        return ExprFilterContainsChild(parentTable, info, filter, childTable, !negated)
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprFilterContainsChild(remapper.remap(parentTable), info, filter?.remap(remapper), remapper.remap(childTable), negated)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
