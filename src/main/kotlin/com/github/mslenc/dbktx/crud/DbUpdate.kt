package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.crud.dsl.ColumnUpdateOps
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.schema.DbEntity

interface DbColumnMutation<E: DbEntity<E, *>, T: Any> {
    operator fun plusAssign(value: T)
    operator fun minusAssign(value: T)
    operator fun timesAssign(value: T)
    operator fun divAssign(value: T)
    operator fun remAssign(value: T)

    operator fun plusAssign(value: Expr<T>)
    operator fun minusAssign(value: Expr<T>)
    operator fun timesAssign(value: Expr<T>)
    operator fun divAssign(value: Expr<T>)
    operator fun remAssign(value: Expr<T>)

    infix fun becomes(value: ColumnUpdateOps<E, T>.()-> Expr<T>)
}

interface DbUpdate<E : DbEntity<E, *>> : DbMutation<E> {
    fun filter(block: FilterBuilder<E>.() -> FilterExpr)

    operator fun <T : Any> get(column: Column<E, T>): DbColumnMutation<E, T>

    fun anyChangesSoFar(): Boolean

    suspend fun execute(): Long
}