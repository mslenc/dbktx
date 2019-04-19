package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.crud.dsl.ColumnUpdateOps
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.schema.DbEntity

interface DbColumnMutation<E: DbEntity<E, *>, T: Any> {
    operator fun plusAssign(value: T)
    operator fun minusAssign(value: T)
    operator fun timesAssign(value: T)
    operator fun divAssign(value: T)
    operator fun remAssign(value: T)

    operator fun plusAssign(value: Expr<E, T>)
    operator fun minusAssign(value: Expr<E, T>)
    operator fun timesAssign(value: Expr<E, T>)
    operator fun divAssign(value: Expr<E, T>)
    operator fun remAssign(value: Expr<E, T>)

    infix fun becomes(value: ColumnUpdateOps<E, T>.()-> Expr<E, T>)
}

interface DbUpdate<E : DbEntity<E, *>> : DbMutation<E> {
    operator fun <T : Any> get(column: Column<E, T>): DbColumnMutation<E, T>

    fun anyChangesSoFar(): Boolean

    suspend fun execute(): Long
}