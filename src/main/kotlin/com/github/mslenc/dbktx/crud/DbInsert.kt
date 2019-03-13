package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.expr.ExprNow
import com.github.mslenc.dbktx.schema.DbEntity

interface DbInsert<E : DbEntity<E, ID>, ID: Any> : DbMutation<E> {
    suspend fun execute(): ID

    fun <T: Any> NOW(): ExprNow<E, T> {
        return ExprNow()
    }

    fun copyUnsetValuesFrom(original: E)
}