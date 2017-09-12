package com.xs0.dbktx.composite

import com.xs0.dbktx.expr.CompositeExpr
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.schema.NonNullColumn

abstract class CompositeId<E : DbEntity<E, ID>, ID : CompositeId<E, ID>> : CompositeExpr<E, ID> {
    abstract override fun equals(other: Any?): Boolean

    abstract override fun hashCode(): Int

    abstract operator fun <X: Any> get(column: NonNullColumn<E, X>): X

    abstract val numColumns: Int
    abstract fun getColumn(index: Int): NonNullColumn<E, *>

    abstract val tableMetainfo: DbTable<E, ID>

    override fun getPart(index: Int): Expr<E, *> {
        return doGetPart(getColumn(index))
    }

    override val numParts: Int
        get() = numColumns

    private fun <T: Any> doGetPart(column: NonNullColumn<E, T>): Expr<E, T> {
        return column.makeLiteral(get(column))
    }
}
