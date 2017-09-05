package com.xs0.dbktx


interface ExprBoolean<TABLE> : Expr<TABLE, Boolean> {
    operator fun not(): ExprBoolean<TABLE> {
        return ExprNegate(this)
    }

    fun and(other: Expr<in TABLE, Boolean>): ExprBoolean<TABLE> {
        return ExprAnd.create(this, other)
    }

    fun or(other: Expr<TABLE, Boolean>): ExprBoolean<TABLE> {
        return ExprOr.create(this, other)
    }
}
