package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

class FilterIsNull<ENTITY>(private val inner: Expr<ENTITY, *>, private val isNull: Boolean) : FilterExpr {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +inner
            if (isNull) {
                raw(" IS NULL")
            } else {
                raw(" IS NOT NULL")
            }
        }
    }

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
