package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

class FilterNegate(private val inner: FilterExpr) : FilterExpr {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +"NOT "
            +inner
        }
    }

    override fun not(): FilterExpr {
        return inner
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterNegate(inner.remap(remapper))
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}