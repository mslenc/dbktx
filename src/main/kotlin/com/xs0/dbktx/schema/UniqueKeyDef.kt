package com.xs0.dbktx.schema

import com.xs0.dbktx.crud.EntityValues
import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.Expr

interface UniqueKeyDef<E: DbEntity<E, *>, T: Any>: NonNullRowProp<E, T> {
    val table: DbTable<E, *>
    val indexInTable: Int
    val numColumns: Int

    val isPrimaryKey: Boolean

    val isComposite: Boolean
        get() {
            return numColumns > 1
        }

    fun getColumn(index: Int): NonNullColumn<E, *>
    operator fun invoke(entity: E): T
}

interface SingleColumnKeyDef<E: DbEntity<E, *>, T: Any>: UniqueKeyDef<E, T> {

}

internal class SingleColumnKeyDefImpl<E: DbEntity<E, *>, T: Any>(
    override val table: DbTable<E, *>,
    override val indexInTable: Int,
    val column: NonNullColumn<E, T>,
    override val isPrimaryKey: Boolean)
    : SingleColumnKeyDef<E, T> {

    override val isComposite: Boolean
        get() = false

    override val numColumns: Int
        get() = 1

    override val isAutoGenerated: Boolean
        get() = column.isAutoGenerated

    override fun extract(values: EntityValues<E>): T? {
        return values.getValue(column)
    }

    override fun getColumn(index: Int): NonNullColumn<E, T> {
        if (index == 1)
            return column

        throw IllegalArgumentException(index.toString() + " is not a valid index")
    }

    override fun makeLiteral(value: T): Expr<E, T> {
        return column.makeLiteral(value)
    }

    override fun bindForSelect(tableInQuery: TableInQuery<E>): Expr<E, T> {
        return column.bindForSelect(tableInQuery)
    }

    override fun invoke(row: List<Any?>): T {
        return column(row)
    }

    override fun invoke(entity: E): T {
        return column(entity)
    }
}