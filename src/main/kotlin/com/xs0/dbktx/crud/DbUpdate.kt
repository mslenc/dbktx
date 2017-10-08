package com.xs0.dbktx.crud

import com.xs0.dbktx.util.defer
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.NonNullColumn
import com.xs0.dbktx.schema.NullableColumn
import kotlinx.coroutines.experimental.Deferred

interface DbUpdate<E : DbEntity<E, *>> : DbMutation<E> {
    suspend fun execute(): Long

    fun executeAsync(): Deferred<Long> {
        return defer { execute() }
    }
}