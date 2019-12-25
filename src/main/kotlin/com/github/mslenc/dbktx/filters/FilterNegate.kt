package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

class FilterNegate(private val inner: Expr<Boolean>) : FilterExpr {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +"NOT "
            +inner
        }
    }

    override val couldBeNull: Boolean
        get() = inner.couldBeNull

    override val involvesAggregation: Boolean
        get() = inner.involvesAggregation

    override fun not(): Expr<Boolean> {
        return inner
    }

    override fun remap(remapper: TableRemapper): Expr<Boolean> {
        return FilterNegate(inner.remap(remapper))
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}