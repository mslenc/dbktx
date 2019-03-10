package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.ManyToOneInfo
import com.github.mslenc.dbktx.schema.NonNullColumn
import com.github.mslenc.dbktx.util.Sql

class FilterOneOfRelToOne<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>
    : FilterExpr {

    private val info: ManyToOneInfo<FROM, TO, *>
    private val refs: List<TO>
    private val negated: Boolean
    private val tableInQuery: TableInQuery<FROM>

    constructor(tableInQuery: TableInQuery<FROM>, info: ManyToOneInfo<FROM, TO, *>, refs: List<TO>, negated: Boolean = false) {
        this.tableInQuery = tableInQuery
        this.info = info
        this.refs = refs
        this.negated = negated
    }

    override fun not(): FilterExpr {
        return FilterOneOfRelToOne(tableInQuery, info, refs, !negated)
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        val multiColumn = info.columnMappings.size > 1

        sql.expr(topLevel) {
            paren(showParens = multiColumn) {
                tuple(info.columnMappings) {
                    +it.bindFrom(tableInQuery)
                }
            }

            +(if (negated) " NOT IN " else " IN ")

            paren {
                tuple(refs) { ref ->
                    paren(showParens = multiColumn) {
                        tuple(info.columnMappings) { colMap ->
                            writeLiteral(colMap.rawColumnTo, ref, sql)
                        }
                    }
                }
            }
        }
    }

    private fun <T: Any>
    writeLiteral(column: NonNullColumn<TO, T>, ref: TO, sql: Sql) {
        column.sqlType.toSql(column(ref), sql)
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterOneOfRelToOne(remapper.remap(tableInQuery), info, refs, negated)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}


