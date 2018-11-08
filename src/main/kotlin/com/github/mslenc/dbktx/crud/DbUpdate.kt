package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.util.vertxDefer
import kotlinx.coroutines.Deferred

interface DbUpdate<E : DbEntity<E, *>> : DbMutation<E> {
    suspend fun execute(): Long

    fun executeAsync(): Deferred<Long> {
        return vertxDefer { execute() }
    }
}