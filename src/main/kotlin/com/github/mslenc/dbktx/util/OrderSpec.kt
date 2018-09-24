package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.schema.DbEntity

internal class OrderSpec<E : DbEntity<E, *>>(
    val table: TableInQuery<E>,
    val expr: Expr<in E, *>,
    val isAscending: Boolean
) {

    fun remap(remapper: TableRemapper): OrderSpec<E> {
        return OrderSpec(remapper.remap(table), expr.remap(remapper), isAscending)
    }
}
