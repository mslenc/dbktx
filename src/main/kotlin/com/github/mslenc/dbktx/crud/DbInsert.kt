package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.expr.ExprNow
import com.github.mslenc.dbktx.schema.DbEntity

interface DbInsert<E : DbEntity<E, ID>, ID: Any> : DbMutation<E> {
    suspend fun execute(): ID

    fun <T: Any> NOW(): ExprNow<T> {
        return ExprNow()
    }

    /**
     * Copies values from original for any columns that don't have their value already set, except for auto-generated
     * primary key columns.
     */
    fun copyUnsetValuesFrom(original: E)
}