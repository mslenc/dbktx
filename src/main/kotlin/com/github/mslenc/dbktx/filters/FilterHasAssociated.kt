package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.JoinType
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.expr.not
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.ManyToOneInfo
import com.github.mslenc.dbktx.util.Sql

class FilterHasAssociated<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>(
        private val parentTable: TableInQuery<FROM>,
        private val info: ManyToOneInfo<TO, FROM, *>,
        private val filter: Expr<Boolean>,
        private val childTable: TableInQuery<TO>,
        private val negated: Boolean = false) : FilterExpr {

    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        val mappings = info.columnMappings
        val n = mappings.size

        // for now, we expect never to get here, if we don't have actual columns (instead, there are CONSTANT or PARAMETER mappings)
        // TODO: we could actually do it - CONSTANTs would become simple restrictions on this table, and PARAMETERS could be ignored,
        // but it's unclear if it'd be actually useful anywhere, so postponing for now
        if (childTable.incomingJoin?.joinType == JoinType.SUB_QUERY) {
            sql.expr(topLevel) {
                sql.subQueryWrapper(negated, needleCanBeNull = false, nullWillBeFalse = nullWillBeFalse) { IN ->
                    paren(n > 1) {
                        tuple(mappings) {
                            sql(it.bindColumnTo(parentTable), false, false)
                        }
                    }
                    +IN
                    +"(SELECT "
                        tuple(mappings) {
                            sql(it.bindColumnFrom(childTable), false, false)
                        }
                        FROM(info.manyTable, childTable.tableAlias)
                        WHERE(filter)
                    +")"
                }
            }
        } else {
            sql.expr(topLevel) {
                if (negated) {
                    sql(filter.not(), nullWillBeFalse, false)
                } else {
                    sql(filter, nullWillBeFalse, false)
                }
            }
        }
    }

    override val couldBeNull: Boolean
        get() = false

    override val involvesAggregation: Boolean
        get() = filter.involvesAggregation

    override fun not(): FilterExpr {
        return FilterHasAssociated(parentTable, info, filter, childTable, !negated)
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterHasAssociated(remapper.remap(parentTable), info, filter.remap(remapper), remapper.remap(childTable), negated)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
