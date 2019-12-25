package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr

internal class OrderSpec(
    val expr: Expr<*>,
    val isAscending: Boolean
) {

    fun remap(remapper: TableRemapper): OrderSpec {
        return OrderSpec(expr.remap(remapper), isAscending)
    }
}
