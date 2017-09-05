package com.xs0.dbktx

class ExprNull<ENTITY, T> : Expr<ENTITY, T> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("NULL")
    }

    fun equalTo(other: Expr<in ENTITY, T>): ExprBoolean<ENTITY> {
        return other.isNull as ExprBoolean<ENTITY>
    }

    fun notEqualTo(other: Expr<in ENTITY, T>): ExprBoolean<ENTITY> {
        return other.isNotNull as ExprBoolean<ENTITY>
    }

    companion object {
        fun <E : DbEntity<E, *>, T> create(): ExprNull<E, T> {
            return ExprNull()
        }
    }
}