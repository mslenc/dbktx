package com.github.mslenc.dbktx.composite

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.CompositeExpr
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.dbktx.schema.NonNullColumn

abstract class CompositeId<E : DbEntity<E, *>, ID : CompositeId<E, ID>> : CompositeExpr<E, ID> {
    abstract override fun equals(other: Any?): Boolean

    abstract override fun hashCode(): Int

    abstract operator fun <X: Any> get(column: NonNullColumn<E, X>): X

    abstract val numColumns: Int
    abstract fun getColumn(index: Int): NonNullColumn<E, *>

    abstract val tableMetainfo: DbTable<E, *>

    override fun getPart(index: Int): Expr<*> {
        return doGetPart(getColumn(index))
    }

    override val numParts: Int
        get() = numColumns

    override val couldBeNull: Boolean
        get() = false

    override val involvesAggregation: Boolean
        get() = false

    private fun <T: Any> doGetPart(column: NonNullColumn<E, T>): Expr<T> {
        return column.makeLiteral(get(column))
    }

    override fun remap(remapper: TableRemapper): Expr<ID> {
        return this
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
