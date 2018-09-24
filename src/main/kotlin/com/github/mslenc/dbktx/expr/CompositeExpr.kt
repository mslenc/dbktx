package com.github.mslenc.dbktx.expr

interface CompositeExpr<ENTITY, TYPE> : Expr<ENTITY, TYPE> {
    val numParts: Int
    fun getPart(index: Int): Expr<ENTITY, *>

    override val isComposite: Boolean
        get() = true
}