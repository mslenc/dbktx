package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.crud.FilterableQuery
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.schema.*

interface AggrExpr<E : DbEntity<E, *>, T: Any>: SqlEmitter
interface NullableAggrExpr<E : DbEntity<E, *>, T: Any> : AggrExpr<E, T>
interface NonNullAggrExpr<E: DbEntity<E, *>, T: Any> : AggrExpr<E, T>

interface AggrBuilder<E : DbEntity<E, *>> : FilterableQuery<E> {
    fun <T: Any> sum(block: AggrExprBuilder<E>.() -> Expr<T>): NullableAggrExpr<E, T>
    fun <T: Any> min(block: AggrExprBuilder<E>.() -> Expr<T>): NullableAggrExpr<E, T>
    fun <T: Any> max(block: AggrExprBuilder<E>.() -> Expr<T>): NullableAggrExpr<E, T>
    fun <T: Any> average(block: AggrExprBuilder<E>.() -> Expr<T>): NullableAggrExpr<E, Double>
    fun <T: Any> count(block: AggrExprBuilder<E>.() -> Expr<T>): NonNullAggrExpr<E, Long>
    fun <T: Any> countDistinct(block: AggrExprBuilder<E>.() -> Expr<T>): NonNullAggrExpr<E, Long>
}

interface AggrStreamBuilder<E: DbEntity<E, *>> : AggrBuilder<E> {
    fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToSingle<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)

    fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToSingle<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)

    infix fun <T: Any> NonNullAggrExpr<E, T>.into(receiver: (T)->Unit)
    infix fun <T: Any> NullableAggrExpr<E, T>.into(receiver: (T?)->Unit)
    infix fun <T: Any> NonNullColumn<E, T>.into(receiver: (T)->Unit)
    infix fun <T: Any> NullableColumn<E, T>.into(receiver: (T?)->Unit)
}

interface AggrStreamTopLevelBuilder<E: DbEntity<E, *>> : AggrStreamBuilder<E> {
    fun onRowStart(block: (DbRow)->Unit)
    fun onRowEnd(block: (DbRow)->Unit)
}

interface AggrInsertSelectBuilder<OUT: DbEntity<OUT, *>, E: DbEntity<E, *>> : AggrBuilder<E> {
    fun <T: Any> expr(block: AggrExprBuilder<E>.() -> Expr<T>): NonNullAggrExpr<E, T>

    fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToSingle<E, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<E, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit)

    fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToSingle<E, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<E, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit)

    infix fun <T: Any> Column<OUT, T>.becomes(expr: AggrExpr<E, T>)

    infix fun <T: Any> NonNullColumn<OUT, T>.becomes(column: NonNullColumn<E, T>)
    infix fun <T: Any> NullableColumn<OUT, T>.becomes(column: Column<E, T>)
}

interface AggrInsertSelectTopLevelBuilder<OUT: DbEntity<OUT, *>, ROOT: DbEntity<ROOT, *>> : AggrInsertSelectBuilder<OUT, ROOT> {
    infix fun <T: Any> NonNullColumn<OUT, T>.becomes(literal: T)
    infix fun <T: Any> NullableColumn<OUT, T>.becomes(literal: T?)
}