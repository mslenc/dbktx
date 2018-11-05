package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.util.Sql
import com.github.mslenc.dbktx.util.StringSet

class ExprFindInSet<E>(private val value: Expr<E, String>,
                       private val set: Expr<E, StringSet>): ExprBoolean {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("FIND_IN_SET(")
        value.toSql(sql, true)
        sql.raw(", ")
        set.toSql(sql, true)
        sql.raw(")")
    }

    override fun not(): ExprBoolean {
        return ExprNegate(this)
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprFindInSet(value.remap(remapper), set.remap(remapper))
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}