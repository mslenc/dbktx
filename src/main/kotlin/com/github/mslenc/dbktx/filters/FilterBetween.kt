package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

class FilterBetween<T: Any>(
        private val value: Expr<T>,
        private val minimum: Expr<T>,
        private val maximum: Expr<T>,
        private val between: Boolean
): FilterExpr {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.expr(topLevel) {
            sql(value, false, false)
            +(if (between) " BETWEEN " else " NOT BETWEEN ")
            sql(minimum, false, false)
            +" AND "
            sql(maximum, false, false)
        }
    }

    override val couldBeNull: Boolean
        get() = value.couldBeNull || minimum.couldBeNull || maximum.couldBeNull

    override val involvesAggregation: Boolean
        get() = value.involvesAggregation || minimum.involvesAggregation || maximum.involvesAggregation

    override operator fun not(): FilterExpr {
        return FilterBetween(value, minimum, maximum, !between)
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterBetween(value.remap(remapper), minimum.remap(remapper), maximum.remap(remapper), between)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
