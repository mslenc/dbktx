package com.xs0.dbktx.expr

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.util.Sql

class ExprNull<ENTITY, T> : Expr<ENTITY, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("NULL")
    }

    override infix fun eq(other: Expr<in ENTITY, T>): ExprBoolean<ENTITY> {
        if (other is NullableExpr) {
            @Suppress("UNCHECKED_CAST")
            other as NullableExpr<ENTITY, T>

            return other.isNull
        } else {
            throw IllegalArgumentException("Equality check between NULL and a non-null expression " + other)
        }
    }

    override infix fun neq(other: Expr<in ENTITY, T>): ExprBoolean<ENTITY> {
        if (other is NullableExpr) {
            @Suppress("UNCHECKED_CAST")
            other as NullableExpr<ENTITY, T>

            return other.isNotNull
        } else {
            throw IllegalArgumentException("Equality check between NULL and a non-null expression " + other)
        }
    }

    companion object {
        fun <E : DbEntity<E, *>, T> create(): ExprNull<E, T> {
            return ExprNull()
        }
    }
}