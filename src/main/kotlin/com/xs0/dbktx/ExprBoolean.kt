package com.xs0.dbktx


interface ExprBoolean<E> : Expr<E, Boolean> {
    operator fun not(): ExprBoolean<E> {
        return ExprNegate(this)
    }

    fun and(other: Expr<in E, Boolean>): ExprBoolean<E> {
        return ExprBools.create(this, ExprBools.Op.AND, other)
    }

    fun or(other: Expr<E, Boolean>): ExprBoolean<E> {
        return ExprBools.create(this, ExprBools.Op.OR, other)
    }
}
