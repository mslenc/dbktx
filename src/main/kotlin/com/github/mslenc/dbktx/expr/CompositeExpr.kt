package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.sqltypes.SqlType
import java.lang.UnsupportedOperationException

interface CompositeExpr<ENTITY, TYPE : Any> : Expr<TYPE> {
    val numParts: Int
    fun getPart(index: Int): Expr<*>

    override fun getSqlType(): SqlType<TYPE> {
        throw UnsupportedOperationException("getSqlType() called on CompositeExpr")
    }

    override val isComposite: Boolean
        get() = true
}