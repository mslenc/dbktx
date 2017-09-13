package com.xs0.dbktx.crud

import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.ExprNull
import com.xs0.dbktx.schema.*

interface DbMutation<E : DbEntity<E, *>> {
    operator fun <T : Any> set(column: Column<E, T>, value: Expr<in E, T>): DbMutation<E>

    operator fun <T : Any> set(column: NullableColumn<E, T>, value: T?): DbMutation<E> {
        return if (value == null) {
            setNull(column)
        } else {
            set(column, column.makeLiteral(value))
        }
    }

    operator fun <T : Any> set(column: NonNullColumn<E, T>, value: T): DbMutation<E> {
        set(column, column.makeLiteral(value))
        return this
    }

    operator fun <T: Any> NullableColumn<E, T>.plusAssign(value: T?) {
        set(this, value)
    }

    operator fun <T: Any> NonNullColumn<E, T>.plusAssign(value: T) {
        set(this, value)
    }

    operator fun <T: Any> NonNullColumn<E, T>.plusAssign(value: Expr<in E, T>) {
        set(this, value)
    }

    fun <T : Any> setNull(column: NullableColumn<E, T>): DbMutation<E> {
        return set(column, ExprNull.create())
    }

    operator fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
            set(relation: RelToOne<E, TARGET>, target: TARGET): DbMutation<E>

    operator fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
        RelToOne<E, TARGET>.plusAssign(target: TARGET) {

        set(this, target)
    }
}