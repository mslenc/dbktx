package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

class FilterIsNull(private val inner: Expr<*>, private val isNull: Boolean) : FilterExpr {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.expr(topLevel) {
            sql(inner, false, false)
            if (isNull) {
                raw(" IS NULL")
            } else {
                raw(" IS NOT NULL")
            }
        }
    }

    override val couldBeNull: Boolean
        get() = false

    override val involvesAggregation: Boolean
        get() = inner.involvesAggregation

    override fun not(): FilterExpr {
        return FilterIsNull(inner, !isNull)
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterIsNull(inner.remap(remapper), isNull)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
