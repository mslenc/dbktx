package com.xs0.dbktx.schema

import com.xs0.dbktx.crud.EntityValues
import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.expr.*

interface RowProp<E : DbEntity<E, *>, T> {
    fun invoke(row: List<Any?>): T?

    val isAutoGenerated: Boolean
    fun extract(values: EntityValues<E>): T?
    fun makeLiteral(value: T): Expr<E, T>

    fun bindForSelect(tableInQuery: TableInQuery<E>): Expr<E, T>
}

interface NullableRowProp<E: DbEntity<E, *>, T> : RowProp<E, T>

interface NonNullRowProp<E: DbEntity<E, *>, T> : RowProp<E, T> {
    override operator fun invoke(row: List<Any?>): T
}

interface OrderedProp<E : DbEntity<E, *>, T : Comparable<T>> : RowProp<E, T>

interface NullableOrderedProp<E: DbEntity<E, *>, T : Comparable<T>> : OrderedProp<E, T>
interface NonNullOrderedProp<E: DbEntity<E, *>, T : Comparable<T>> : OrderedProp<E, T>