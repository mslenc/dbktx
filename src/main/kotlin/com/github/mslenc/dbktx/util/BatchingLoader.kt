package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbLoaderInternal
import com.github.mslenc.dbktx.schema.DbTable
import kotlinx.coroutines.CoroutineScope
import mu.KLogging

/**
 * Used for batching requests for data together, to reduce the number of queries performed.
 * Definitions are expected to be objects/singletons (or in any case, to compare as equal
 * if they are to be batched together).
 */
interface BatchingLoader<KEY, RESULT> {
    /**
     * Called when a batch is ready to be loaded, to perform the actual data loading.
     * Any keys in input not present in output will be resolved with nullResult(),
     * so there is usually no need to do this check manually.
     */
    suspend fun loadNow(keys: Set<KEY>, db: DbConn): Map<KEY, RESULT>

    /**
     * The value to be used when loadNow() doesn't provide one for some key. Typical
     * values (depending on what RESULT is) are null or an empty collection.
     */
    fun nullResult(): RESULT

    /**
     * Called when flushing caches, to determine whether to flush this loader's cache
     * too. The table passed in as a parameter is the one in which changes happened
     * (insert/update/delete). The result should be true if the cache for this loader
     * should be flushed. When unsure, conservatively return true, which is also the
     * default implementation.
     */
    fun isRelated(table: DbTable<*, *>): Boolean = true
}

internal class BatchingLoaderIndex<KEY: Any, RESULT>(val loader: BatchingLoader<KEY, RESULT>, val scope: CoroutineScope) {

    private var index: HashMap<KEY, DelayedLoadState<RESULT>> = HashMap()
    private var keysToLoad = LinkedHashSet<KEY>()

    operator fun get(key: KEY): DelayedLoadState<RESULT> {
        return index.computeIfAbsent(key) { DelayedLoadState(scope) }
    }

    fun addKeyToLoad(key: KEY) {
        keysToLoad.add(key)
    }

    fun getAndClearKeysToLoad(): MutableSet<KEY>? {
        if (keysToLoad.isEmpty())
            return null

        val res = keysToLoad
        keysToLoad = LinkedHashSet()
        return res
    }

    fun reportNull(ids: Set<KEY>) {
        for (id in ids) {
            index[id]?.handleResult(loader.nullResult())
        }
    }

    fun reportError(ids: Set<KEY>, error: Throwable) {
        for (id in ids) {
            index[id]?.handleError(error)
        }
    }

    fun flushAll(cleanUp: ArrayList<()->Unit>) {
        // if there are any outstanding keys to load, we will fail them - obviously,
        // they either got confused about timing of stuff, or decided that there's no
        // point in waiting for something..

        val index = this.index
        this.index = HashMap()

        val keysToLoad = this.keysToLoad
        if (keysToLoad.isEmpty())
            return

        this.keysToLoad = LinkedHashSet()

        cleanUp.add {
            val error = IllegalStateException("The cache has been flushed")
            for (key in keysToLoad) {
                try {
                    index[key]?.handleError(error)
                } catch (e: Throwable) {
                    logger.error("Handler error when flushing cache", e)
                    // but continue anyway..
                }
            }
        }
    }

    companion object : KLogging()

    internal suspend fun loadNow(dbLoaderImpl: DbLoaderInternal): Boolean {
        return dbLoaderImpl.loadCustomBatchNow(this)
    }
}
