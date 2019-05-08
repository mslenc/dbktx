package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.dbktx.schema.RelToMany
import com.github.mslenc.dbktx.schema.RelToManyImpl
import kotlinx.coroutines.CoroutineScope
import mu.KLogging

internal class MasterIndex(val scope: CoroutineScope) {

    private val tableIndex = LinkedHashMap<DbTable<*, *>, EntityIndex<*>>()
    private val relToManyIndex = LinkedHashMap<RelToManyImpl<*, *, *>, ToManyIndex<*, *, *>>()
    private val loaderIndex = LinkedHashMap<BatchingLoader<*, *>, BatchingLoaderIndex<*, *>>()

    fun flushAll() {
        // (When we flush, we fail any outstanding request. However, handlers for those failures
        // may initiate new queries etc, so we clean up the cache first, and call the handlers
        // later, and so eventual new queries will all go to the fresh cache)
        val cleanUp = ArrayList<()->Unit>()

        for (index in tableIndex.values)
            index.flushAll(cleanUp)

        for (index in relToManyIndex.values)
            index.flushAll(cleanUp)

        for (index in loaderIndex.values)
            index.flushAll(cleanUp)

        for (runnable in cleanUp) {
            try {
                runnable()
            } catch (e: Throwable) {
                logger.error("Error while cleaning up", e)
                // but continue
            }
        }
    }

    fun <E : DbEntity<E, ID>, ID: Any> flushRelated(table: DbTable<E, ID>) {
        val cleanUp = ArrayList<()->Unit>()

        tableIndex[table]?.flushAll(cleanUp)

        for ((rel, index) in relToManyIndex) {
            if (rel.sourceTable === table || rel.targetTable === table) {
                index.flushAll(cleanUp)
            }
        }

        for ((loader, index) in loaderIndex) {
            if (loader.isRelated(table)) {
                index.flushAll(cleanUp)
            }
        }

        for (runnable in cleanUp) {
            try {
                runnable()
            } catch (e: Throwable) {
                logger.error("Error while cleaning up", e)
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <E : DbEntity<E, *>>
    get(table: DbTable<E, *>): EntityIndex<E> {
        return tableIndex.computeIfAbsent(table) { EntityIndex(table, scope) } as EntityIndex<E>
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <FROM: DbEntity<FROM, *>, FROM_KEY: Any, TO: DbEntity<TO, *>>
    get(rel: RelToMany<FROM, TO>): ToManyIndex<FROM, FROM_KEY, TO> {
        rel as RelToManyImpl<FROM, FROM_KEY, TO>

        return relToManyIndex.computeIfAbsent(rel)
               { ToManyIndex(rel, scope) }
               as ToManyIndex<FROM, FROM_KEY, TO>
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <KEY: Any, RESULT>
    get(loader: BatchingLoader<KEY, RESULT>): BatchingLoaderIndex<KEY, RESULT> {

        return loaderIndex.computeIfAbsent(loader) { BatchingLoaderIndex(loader, scope) } as BatchingLoaderIndex<KEY, RESULT>
    }

    val allCachedTables: Collection<EntityIndex<*>>
        get() = tableIndex.values

    val allCachedToManyRels: Collection<ToManyIndex<*, *, *>>
        get() = relToManyIndex.values

    val allCachedLoaders: Collection<BatchingLoaderIndex<*, *>>
        get() = loaderIndex.values

    companion object : KLogging()
}