package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.schema.*

interface DbMutation<E : DbEntity<E, *>> {
    val table: TableInQuery<E>

    operator fun <T : Any> set(column: NonNullColumn<E, T>, value: T)
    operator fun <T : Any> set(column: NullableColumn<E, T>, value: T?)
    operator fun <T : Any> set(column: Column<E, T>, value: Expr<T>)

    operator fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
        set(relation: RelToOne<E, TARGET>, target: TARGET)
}