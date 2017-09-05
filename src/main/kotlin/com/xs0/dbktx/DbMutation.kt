package com.xs0.dbktx

interface DbMutation<E : DbEntity<E, *>> {
    operator fun <T : Any> set(column: Column<E, T>, value: Expr<in E, T>): DbMutation<E>

    operator fun <T : Any> set(column: Column<E, T>, value: T?): DbMutation<E> {
        return if (value == null) {
            setNull(column)
        } else {
            set(column, column.makeLiteral(value))
        }
    }

    fun <T : Any> setNull(column: Column<E, T>): DbMutation<E> {
        return set(column, ExprNull.create())
    }

    operator fun <TARGET : DbEntity<TARGET, TID>, TID> set(relation: RelToOne<E, TARGET>, target: TARGET): DbMutation<E>
}