package com.xs0.dbktx

import kotlinx.coroutines.experimental.Deferred

interface DbUpdate<E : DbEntity<E, *>> : DbMutation<E> {
    suspend fun execute(): Long

    fun executeAsync(): Deferred<Long> {
        return defer { execute() }
    }
}