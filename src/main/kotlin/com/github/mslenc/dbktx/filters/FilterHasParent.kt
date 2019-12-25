package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.JoinType
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.ManyToOneInfo
import com.github.mslenc.dbktx.util.Sql

class FilterHasParent<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>(
        private val info: ManyToOneInfo<FROM, TO, *>,
        private val filter: Expr<Boolean>,
        private val srcTable: TableInQuery<FROM>,
        private val dstTable: TableInQuery<TO>,
        private val negated: Boolean = false) : FilterExpr {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        val mappings = info.columnMappings
        val n = mappings.size

        val needleCanBeNull = info.columnMappings.any { it.columnFromAsNullable != null }

        if (dstTable.incomingJoin?.joinType == JoinType.SUB_QUERY) {
            sql.expr(topLevel) {
                sql.subQueryWrapper(negated, needleCanBeNull) { IN ->
                    paren(n > 1) {
                        tuple(info.columnMappings) {
                            +it.bindFrom(srcTable)
                        }
                    }
                    +IN
                    +"(SELECT "
                    tuple(info.columnMappings) {
                        columnForSelect(it.bindColumnTo(dstTable))
                    }
                    FROM(dstTable)
                    WHERE(filter)
                    +")"
                }
            }
        } else {
            sql.expr(topLevel) {
                +filter
            }
        }
    }

    override val couldBeNull: Boolean
        get() = false

    override val involvesAggregation: Boolean
        get() = filter.involvesAggregation

    override fun not(): FilterExpr {
        return FilterHasParent(info, filter, srcTable, dstTable, !negated)
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterHasParent(info, filter.remap(remapper), remapper.remap(srcTable), remapper.remap(dstTable), negated)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
