package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.util.Sql

class ExprNegate(private val inner: ExprBoolean) : ExprBoolean {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +"NOT "
            +inner
        }
    }

    override fun not(): ExprBoolean {
        return inner
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprNegate(inner.remap(remapper))
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}