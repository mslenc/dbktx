package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.util.Sql

class ExprNull<ENTITY, T> : Expr<ENTITY, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("NULL")
    }

    override fun remap(remapper: TableRemapper): Expr<ENTITY, T> {
        return this
    }

    companion object {
        fun <E : DbEntity<E, *>, T> create(): ExprNull<E, T> {
            return ExprNull()
        }
    }
}