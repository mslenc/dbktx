package com.xs0.dbktx.util

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.schema.DbEntity

internal class OrderSpec<E : DbEntity<E, *>>(
    val table: TableInQuery<E>,
    val expr: Expr<in E, *>,
    val isAscending: Boolean
) {

    fun remap(remapper: TableRemapper): OrderSpec<E> {
        return OrderSpec(remapper.remap(table), expr.remap(remapper), isAscending)
    }
}
