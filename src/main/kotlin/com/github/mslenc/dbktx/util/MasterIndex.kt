package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.dbktx.schema.RelToMany
import com.github.mslenc.dbktx.schema.RelToManyImpl
import mu.KLogging

internal class MasterIndex {

    private val tableIndex = LinkedHashMap<DbTable<*, *>, EntityIndex<*>>()
    private val relToManyIndex = LinkedHashMap<RelToManyImpl<*, *, *>, ToManyIndex<*, *, *>>()

    fun flushAll() {
        // (When we flush, we fail any outstanding request. However, handlers for those failures
        // may initiate new queries etc, so we clean up the cache first, and call the handlers
        // later, and so eventual new queries will all go to the fresh cache)
        val cleanUp = ArrayList<()->Unit>()

        for (index in tableIndex.values)
            index.flushAll(cleanUp)

        for (index in relToManyIndex.values)
            index.flushAll(cleanUp)

        for (runnable in cleanUp) {
            try {
                runnable()
            } catch (e: Exception) {
                logger.error("Error while cleaning up", e)
                // but continue
            }
        }
    }

    fun <E : DbEntity<E, ID>, ID: Any> flushRelated(table: DbTable<E, ID>) {
        val cleanUp = ArrayList<()->Unit>()

        tableIndex[table]?.flushAll(cleanUp)

        for ((key, value) in relToManyIndex) {
            if (key.sourceTable === table || key.targetTable === table) {
                value.flushAll(cleanUp)
            }
        }

        for (runnable in cleanUp) {
            try {
                runnable()
            } catch (e: Exception) {
                logger.error("Error while cleaning up", e)
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <E : DbEntity<E, *>>
    get(table: DbTable<E, *>): EntityIndex<E> {
        return tableIndex.computeIfAbsent(table) { _ -> EntityIndex(table) } as EntityIndex<E>
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <FROM: DbEntity<FROM, *>, FROM_KEY: Any, TO: DbEntity<TO, *>>
    get(rel: RelToMany<FROM, TO>): ToManyIndex<FROM, FROM_KEY, TO> {
        rel as RelToManyImpl<FROM, FROM_KEY, TO>

        return relToManyIndex.computeIfAbsent(rel)
               { _ -> ToManyIndex(rel) }
               as ToManyIndex<FROM, FROM_KEY, TO>
    }

    val allCachedTables: Collection<EntityIndex<*>>
        get() = tableIndex.values

    val allCachedToManyRels: Collection<ToManyIndex<*, *, *>>
        get() = relToManyIndex.values

    companion object : KLogging()
}