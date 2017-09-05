package com.xs0.dbktx


interface Expr<ENTITY, TYPE> {
    fun toSql(sb: SqlBuilder, topLevel: Boolean)

    infix fun eq(other: Expr<in ENTITY, TYPE>): ExprBoolean<ENTITY> {
        return ExprEquals.equals(this, other)
    }

    infix fun neq(other: Expr<in ENTITY, TYPE>): ExprBoolean<ENTITY> {
        return ExprEquals.notEquals(this, other)
    }

    val isComposite: Boolean
        get() = false

    val isNull: ExprBoolean<ENTITY>
        get() = ExprIsNull(this)

    val isNotNull: ExprBoolean<ENTITY>
        get() = ExprIsNotNull(this)
}
