package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.JoinType
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.ManyToOneInfo
import com.github.mslenc.dbktx.util.Sql

class ExprFilterHasParent<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>(
        private val info: ManyToOneInfo<FROM, TO, *>,
        private val filter: ExprBoolean,
        private val srcTable: TableInQuery<FROM>,
        private val dstTable: TableInQuery<TO>,
        private val negated: Boolean = false) : ExprBoolean {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        val mappings = info.columnMappings
        val n = mappings.size

        if (dstTable.incomingJoin?.joinType == JoinType.SUB_QUERY) {
            sql.expr(topLevel) {
                paren(n > 1) {
                    tuple(info.columnMappings) {
                        +it.bindFrom(srcTable)
                    }
                }
                +(if (negated) " NOT IN " else " IN ")
                +"(SELECT "
                tuple(info.columnMappings) {
                    columnForSelect(it.bindColumnTo(dstTable))
                }
                FROM(dstTable)
                WHERE(filter)
                +")"
            }
        } else {
            sql.expr(topLevel) {
                +filter
            }
        }
    }

    override fun not(): ExprBoolean {
        return ExprFilterHasParent(info, filter, srcTable, dstTable, !negated)
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprFilterHasParent(info, filter.remap(remapper), remapper.remap(srcTable), remapper.remap(dstTable), negated)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}