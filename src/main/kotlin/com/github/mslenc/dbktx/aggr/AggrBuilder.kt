package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.crud.FilterBuilder
import com.github.mslenc.dbktx.crud.FilterableQuery
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.schema.*
import kotlin.reflect.KMutableProperty1

interface NullableAggrExpr<E : DbEntity<E, *>, T: Any>
interface NonNullAggrExpr<E: DbEntity<E, *>, T: Any>

interface AggrBuilder<E : DbEntity<E, *>> : FilterableQuery<E> {
    fun <T: Any> sum(block: AggrExprBuilder<E>.() -> Expr<E, T>): NullableAggrExpr<E, T>
    fun <T: Any> min(block: AggrExprBuilder<E>.() -> Expr<E, T>): NullableAggrExpr<E, T>
    fun <T: Any> max(block: AggrExprBuilder<E>.() -> Expr<E, T>): NullableAggrExpr<E, T>
    fun <T: Any> average(block: AggrExprBuilder<E>.() -> Expr<E, T>): NullableAggrExpr<E, Double>
    fun <T: Any> count(block: AggrExprBuilder<E>.() -> Expr<E, T>): NonNullAggrExpr<E, Long>
    fun <T: Any> countDistinct(block: AggrExprBuilder<E>.() -> Expr<E, T>): NonNullAggrExpr<E, Long>
}

interface AggrListBuilder<OUT: Any, E: DbEntity<E, *>> : AggrBuilder<E> {
    fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToOne<E, REF>, block: AggrListBuilder<OUT, REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<E, REF>, block: AggrListBuilder<OUT, REF>.() -> Unit)

    fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToOne<E, REF>, block: AggrListBuilder<OUT, REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<E, REF>, block: AggrListBuilder<OUT, REF>.() -> Unit)

    infix fun <T: Any> KMutableProperty1<OUT, T>.becomes(expr: NonNullAggrExpr<E, T>)
    infix fun <T: Any> KMutableProperty1<OUT, T?>.becomes(expr: NullableAggrExpr<E, T>)
    infix fun <T: Any> KMutableProperty1<OUT, T>.becomes(column: NonNullColumn<E, T>)
    infix fun <T: Any> KMutableProperty1<OUT, T?>.becomes(column: NullableColumn<E, T>)
}

interface AggrStreamBuilder<E: DbEntity<E, *>> : AggrBuilder<E> {
    fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToOne<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)

    fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToOne<E, REF>, block: AggrStreamBuilder<REF>.() -> Unit)
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