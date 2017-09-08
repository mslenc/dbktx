package com.xs0.dbktx.crud

import com.xs0.dbktx.util.defer
import com.xs0.dbktx.schema.DbEntity
import kotlinx.coroutines.experimental.Deferred

interface DbInsert<E : DbEntity<E, ID>, ID: Any> : DbMutation<E> {
    suspend fun execute(): ID

    fun executeAsync(): Deferred<ID> = defer { execute() }
}