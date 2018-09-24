package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.schema.*

interface DbMutation<E : DbEntity<E, *>> {
    val table: TableInQuery<E>

    fun <T : Any> set(column: NonNullColumn<E, T>, value: T): DbMutation<E>
    fun <T : Any> set(column: NullableColumn<E, T>, value: T?): DbMutation<E>
    fun <T : Any> set(column: Column<E, T>, value: Expr<E, T>): DbMutation<E>
    fun <T : Any> setNull(column: NullableColumn<E, T>): DbMutation<E>

    operator fun <T: Any> NullableColumn<E, T>.plusAssign(value: T?) {
        set(this, value)
    }

    operator fun <T: Any> NonNullColumn<E, T>.plusAssign(value: T) {
        set(this, value)
    }

    operator fun <T: Any> Column<E, T>.plusAssign(value: Expr<E, T>) {
        set(this, value)
    }

    fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
        set(relation: RelToOne<E, TARGET>, target: TARGET): DbMutation<E>

    operator fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
        RelToOne<E, TARGET>.plusAssign(target: TARGET) {

        set(this, target)
    }
}