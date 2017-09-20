package com.xs0.dbktx.crud

import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.ExprNull
import com.xs0.dbktx.schema.Column
import com.xs0.dbktx.schema.DbEntity

class EntityValues<E : DbEntity<E, *>> : Iterable<Column<E, *>> {
    private val values: MutableMap<Column<E, *>, Any?> = LinkedHashMap()
    private val exprs: MutableMap<Column<E, *>, Expr<E, *>> = LinkedHashMap()

    fun <T : Any> set(col: Column<E, T>, value: T?) {
        values[col] = value
        exprs[col] = if (value == null) ExprNull.create() else col.makeLiteral(value)
    }

    fun <T : Any> set(col: Column<E, T>, value: Expr<E, T>) {
        exprs[col] = value
    }

    fun isEmpty(): Boolean {
        return exprs.isEmpty()
    }

    override operator fun iterator(): Iterator<Column<E, *>> {
        return exprs.keys.iterator()
    }

    fun <T: Any> getValue(column: Column<E, T>): T? {
        @Suppress("UNCHECKED_CAST")
        return values[column] as T?
    }

    fun <T: Any> getExpr(column: Column<E, T>): Expr<E, T>? {
        @Suppress("UNCHECKED_CAST")
        return exprs[column] as Expr<E, T>?
    }
}