package com.xs0.dbktx.expr

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.util.Sql

class ExprNull<ENTITY, T> : Expr<ENTITY, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("NULL")
    }

    companion object {
        fun <E : DbEntity<E, *>, T> create(): ExprNull<E, T> {
            return ExprNull()
        }
    }
}