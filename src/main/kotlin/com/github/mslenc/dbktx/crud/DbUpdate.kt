package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.util.defer
import com.github.mslenc.dbktx.schema.DbEntity
import kotlinx.coroutines.Deferred

interface DbUpdate<E : DbEntity<E, *>> : DbMutation<E> {
    suspend fun execute(): Long

    fun executeAsync(): Deferred<Long> {
        return defer { execute() }
    }
}