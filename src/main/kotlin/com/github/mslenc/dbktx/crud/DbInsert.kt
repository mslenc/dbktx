package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.expr.ExprNow
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

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

interface InsertManyRow<E : DbEntity<E, ID>, ID : Any> : DbMutation<E> {
    fun <T: Any> NOW(): ExprNow<T> {
        return ExprNow()
    }

    /**
     * Copies values from original for any columns that don't have their value already set, except for auto-generated
     * primary key columns.
     */
    fun copyUnsetValuesFrom(original: E)
}

interface InsertManyQuery<E : DbEntity<E, ID>, ID: Any, TABLE: DbTable<E, ID>> {
    suspend fun execute() // TODO: return the list of ids?

    fun addRow(): InsertManyRow<E, ID>

    fun buildRow(block: TABLE.(InsertManyRow<E, ID>)->Unit)
}