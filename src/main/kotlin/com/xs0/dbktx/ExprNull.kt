package com.xs0.dbktx

class ExprNull<ENTITY, T> : Expr<ENTITY, T> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("NULL")
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