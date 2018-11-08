package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.expr.ExprNow
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.util.vertxDefer
import kotlinx.coroutines.Deferred

interface DbInsert<E : DbEntity<E, ID>, ID: Any> : DbMutation<E> {
    suspend fun execute(): ID

    fun executeAsync(): Deferred<ID> = vertxDefer { execute() }

    fun <T> NOW(): ExprNow<E, T> {
        return ExprNow()
    }
}