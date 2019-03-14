package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.expr.BinaryOp
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprBinary
import com.github.mslenc.dbktx.expr.ExprCoalesce
import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.RelToMany
import com.github.mslenc.dbktx.schema.RelToOne

internal class RelPathImpl<BASE: DbEntity<BASE, *>, LAST: DbEntity<LAST, *>>(val tableInQuery: TableInQuery<LAST>) : RelPath<BASE, LAST>

internal class AggrExprBuilderImpl<E: DbEntity<E, *>>(val tableInQuery: TableInQuery<E>) : AggrExprBuilder<E> {
    override fun <T : Any> Column<E, T>.itself(): Expr<E, T> {
        return this.bindForSelect(tableInQuery)
    }

    override fun <T : Any>
    binary(left: Expr<E, T>, op: BinaryOp, right: Expr<E, T>): Expr<E, T> {
        return ExprBinary(left, op, right)
    }

    override fun <MID : DbEntity<MID, *>, T : Any>
    RelToOne<E, MID>.rangeTo(column: Column<MID, T>): Expr<E, T> {
        val nextTable = tableInQuery.innerJoin(this)
        val boundColumn = column.bindForSelect(nextTable)
        @Suppress("UNCHECKED_CAST")
        return boundColumn as Expr<E, T>
    }

    override fun <MID : DbEntity<MID, *>, T : Any>
    RelToMany<E, MID>.rangeTo(column: Column<MID, T>): Expr<E, T> {
        val nextTable = tableInQuery.innerJoin(this)
        val boundColumn = column.bindForSelect(nextTable)
        @Suppress("UNCHECKED_CAST")
        return boundColumn as Expr<E, T>
    }

    override fun <MID : DbEntity<MID, *>, T : Any>
    RelPath<E, MID>.rangeTo(column: Column<MID, T>): Expr<E, T> {
        val path = this as RelPathImpl
        val boundColumn = column.bindForSelect(path.tableInQuery)
        @Suppress("UNCHECKED_CAST")
        return boundColumn as Expr<E, T>
    }

    override fun <MID : DbEntity<MID, *>, NEXT : DbEntity<NEXT, *>>
    RelPath<E, MID>.rangeTo(relToOne: RelToOne<MID, NEXT>): RelPath<E, NEXT> {
        val path = this as RelPathImpl
        val nextTable = path.tableInQuery.innerJoin(relToOne)
        return RelPathImpl(nextTable)
    }

    override fun <MID : DbEntity<MID, *>, NEXT : DbEntity<NEXT, *>>
    RelPath<E, MID>.rangeTo(relToMany: RelToMany<MID, NEXT>): RelPath<E, NEXT> {
        val path = this as RelPathImpl
        val nextTable = path.tableInQuery.innerJoin(relToMany)
        return RelPathImpl(nextTable)
    }

    override fun <MID : DbEntity<MID, *>, NEXT : DbEntity<NEXT, *>>
    RelToOne<E, MID>.rangeTo(relToOne: RelToOne<MID, NEXT>): RelPath<E, NEXT> {
        val midTable = tableInQuery.innerJoin(this)
        val nextTable = midTable.innerJoin(relToOne)
        return RelPathImpl(nextTable)
    }

    override fun <MID : DbEntity<MID, *>, NEXT : DbEntity<NEXT, *>>
    RelToOne<E, MID>.rangeTo(relToMany: RelToMany<MID, NEXT>): RelPath<E, NEXT> {
        val midTable = tableInQuery.innerJoin(this)
        val nextTable = midTable.innerJoin(relToMany)
        return RelPathImpl(nextTable)
    }

    override fun <MID : DbEntity<MID, *>, NEXT : DbEntity<NEXT, *>>
    RelToMany<E, MID>.rangeTo(relToOne: RelToOne<MID, NEXT>): RelPath<E, NEXT> {
        val midTable = tableInQuery.innerJoin(this)
        val nextTable = midTable.innerJoin(relToOne)
        return RelPathImpl(nextTable)
    }

    override fun <MID : DbEntity<MID, *>, NEXT : DbEntity<NEXT, *>>
    RelToMany<E, MID>.rangeTo(relToMany: RelToMany<MID, NEXT>): RelPath<E, NEXT> {
        val midTable = tableInQuery.innerJoin(this)
        val nextTable = midTable.innerJoin(relToMany)
        return RelPathImpl(nextTable)
    }

    override fun <T : Any> coalesce(vararg options: Expr<E, T>, ifAllNull: T?): Expr<E, T> {
        return ExprCoalesce.create(options.toList(), ifAllNull)
    }
}