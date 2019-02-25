package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.schema.DbEntity

interface DbUpdate<E : DbEntity<E, *>> : DbMutation<E> {
    suspend fun execute(): Long
}