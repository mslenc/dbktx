package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.utils.getLogger

class DbCache {

    private val tableIndex = LinkedHashMap<DbTable<*, *>, DbEntityCache<*>>()
    private val loaderIndex = LinkedHashMap<BatchingLoader<*, *>, DbBatchCache<*, *>>()

    fun flushAll() {
        // (When we flush, we fail any outstanding request. However, handlers for those failures
        // may initiate new queries etc, so we clean up the cache first, and call the handlers
        // later, and so eventual new queries will all go to the fresh cache)
        val cleanUp = ArrayList<()->Unit>()

        for (index in tableIndex.values)
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

    fun <E : DbEntity<E, *>> flushRelated(table: DbTable<E, *>) {
        val cleanUp = ArrayList<()->Unit>()

        tableIndex[table]?.flushAll(cleanUp)

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
    internal operator fun <E : DbEntity<E, *>>
    get(table: DbTable<E, *>): DbEntityCache<E> {
        return tableIndex.computeIfAbsent(table) { DbEntityCache(table) } as DbEntityCache<E>
    }

    @Suppress("UNCHECKED_CAST")
    internal operator fun <KEY: Any, RESULT>
    get(loader: BatchingLoader<KEY, RESULT>): DbBatchCache<KEY, RESULT> {
        return loaderIndex.computeIfAbsent(loader) { DbBatchCache(loader) } as DbBatchCache<KEY, RESULT>
    }

    internal val allCachedTables: Collection<DbEntityCache<*>>
        get() = tableIndex.values

    companion object {
        val logger = getLogger<DbCache>()
    }
}