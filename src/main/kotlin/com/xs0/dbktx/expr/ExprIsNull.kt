package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.util.Sql

class ExprIsNull<ENTITY>(private val inner: Expr<ENTITY, *>, private val isNull: Boolean) : ExprBoolean {
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

    override fun not(): ExprBoolean {
        return ExprIsNull(inner, !isNull)
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprIsNull(inner.remap(remapper), isNull)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
