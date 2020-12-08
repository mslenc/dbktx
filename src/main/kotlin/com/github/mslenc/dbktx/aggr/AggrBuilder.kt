package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.crud.FilterableQuery
import com.github.mslenc.dbktx.expr.*
import com.github.mslenc.dbktx.schema.*


interface AggrStreamBuilder<E: DbEntity<E, *>> : AggrExprBuilder<E>, FilterableQuery<E> {
    fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToSingle<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)

    fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToSingle<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)

    fun <T: Any> expr(block: AggrExprBuilder<E>.() -> Expr<T>): Expr<T>

    infix fun <T: Any> Expr<T>.into(receiver: (T?)->Unit)
    infix fun <T: Any> NonNullAggrExpr<T>.intoNN(receiver: (T)->Unit)
    infix fun <T: Any> NonNullColumn<E, T>.intoNN(receiver: (T)->Unit)
    infix fun <T: Any> NullableColumn<E, T>.into(receiver: (T?)->Unit)
}

interface AggrStreamTopLevelBuilder<E: DbEntity<E, *>> : AggrStreamBuilder<E> {
    fun onRowStart(block: (DbRow)->Unit)
    fun onRowEnd(block: (DbRow)->Unit)
}

interface AggrInsertSelectBuilder<OUT: DbEntity<OUT, *>, E: DbEntity<E, *>> : AggrExprBuilder<E>, FilterableQuery<E> {
    fun <T: Any> expr(block: AggrExprBuilder<E>.() -> Expr<T>): Expr<T>

    fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToSingle<E, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<E, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit)

    fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToSingle<E, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<E, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit)

    infix fun <T: Any> Column<OUT, T>.becomes(expr: Expr<T>)

    infix fun <T: Any> NonNullColumn<OUT, T>.becomes(column: NonNullColumn<E, T>)
    infix fun <T: Any> NullableColumn<OUT, T>.becomes(column: Column<E, T>)
}

interface AggrInsertSelectTopLevelBuilder<OUT: DbEntity<OUT, *>, ROOT: DbEntity<ROOT, *>> : AggrInsertSelectBuilder<OUT, ROOT> {
    infix fun <T: Any> NonNullColumn<OUT, T>.becomes(literal: T)
    infix fun <T: Any> NullableColumn<OUT, T>.becomes(literal: T?)
}