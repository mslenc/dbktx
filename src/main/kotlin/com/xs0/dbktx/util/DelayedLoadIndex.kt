package com.xs0.dbktx.util

import com.xs0.dbktx.conn.DbLoaderImpl
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.schema.RelToManyImpl
import kotlinx.coroutines.experimental.sync.Mutex
import mu.KLogging

import java.util.*
import kotlin.collections.HashSet

internal class EntityIndex<E : DbEntity<E, ID>, ID: Any>(
        val metainfo: DbTable<E, ID>) {

    internal val mutex = Mutex()
    private var index: HashMap<ID, DelayedLoadStateNullable<E>> = HashMap()
    private var idsToLoad = LinkedHashSet<ID>()

    operator fun get(id: ID): DelayedLoadStateNullable<E> {
        return index.computeIfAbsent(id) { _ -> DelayedLoadStateNullable() }
    }

    fun addIdToLoad(id: ID) {
        idsToLoad.add(id)
    }

    fun getAndClearIdsToLoad(): MutableSet<ID>? {
        if (idsToLoad.isEmpty())
            return null

        val res = idsToLoad
        idsToLoad = LinkedHashSet()

        return res
    }

    fun reportNull(ids: Set<ID>) {
        for (id in ids)
            index[id]?.handleResult(null)
    }

    fun reportError(ids: Set<ID>, error: Throwable) {
        for (id in ids) {
            index[id]?.handleError(error)
        }
    }

    fun flushAll(cleanUp: ArrayList<()->Unit>) {
        // if there are any outstanding IDs to load, we will fail them - obviously,
        // they either got confused about timing of stuff, or decided that there's no
        // point in waiting for something..

        val index = this.index
        this.index = HashMap()

        val idsToLoad = this.idsToLoad
        this.idsToLoad = LinkedHashSet()

        if (idsToLoad.isEmpty())
            return

        cleanUp.add({
            val error = IllegalStateException("The cache has been flushed")
            for (id in idsToLoad) {
                try {
                    index[id]?.handleError(error)
                } catch (e: Exception) {
                    logger.error("Handler error when flushing cache", e)
                    // but continue anyway..
                }

            }
        })
    }

    companion object : KLogging()

    internal suspend fun loadNow(dbLoaderImpl: DbLoaderImpl): Boolean {
        return dbLoaderImpl.loadDelayedTable(this)
    }

    fun removeIdToLoad(id: ID): Boolean {
        return idsToLoad.remove(id)
    }
}

internal class ToManyIndex<FROM: DbEntity<FROM, FROMID>, FROMID: Any, TO: DbEntity<TO, TOID>, TOID: Any>(
        val relation: RelToManyImpl<FROM, FROMID, TO, TOID>) {

    private var index: HashMap<FROMID, DelayedLoadState<List<TO>>> = HashMap()
    private var idsToLoad = LinkedHashSet<FROMID>()

    operator fun get(id: FROMID): DelayedLoadState<List<TO>> {
        return index.computeIfAbsent(id) { _ -> DelayedLoadState() }
    }

    fun addIdToLoad(id: FROMID) {
        idsToLoad.add(id)
    }

    fun getAndClearIdsToLoad(): MutableSet<FROMID>? {
        if (idsToLoad.isEmpty())
            return null;

        val res = idsToLoad
        idsToLoad = LinkedHashSet()
        return res
    }

    fun reportNull(ids: Set<FROMID>) {
        for (id in ids) {
            index[id]?.handleResult(emptyList())
        }
    }

    fun reportError(ids: Set<FROMID>, error: Throwable) {
        for (id in ids) {
            index[id]?.handleError(error)
        }
    }

    fun flushAll(cleanUp: ArrayList<()->Unit>) {
        // if there are any outstanding IDs to load, we will fail them - obviously,
        // they either got confused about timing of stuff, or decided that there's no
        // point in waiting for something..

        val index = this.index
        this.index = HashMap()

        val idsToLoad = this.idsToLoad
        this.idsToLoad = LinkedHashSet()

        if (idsToLoad.isEmpty())
            return

        cleanUp.add({
            val error = IllegalStateException("The cache has been flushed")
            for (id in idsToLoad) {
                try {
                    index[id]?.handleError(error)
                } catch (e: Exception) {
                    logger.error("Handler error when flushing cache", e)
                    // but continue anyway..
                }

            }
        })
    }

    companion object : KLogging()

    internal suspend fun loadNow(dbLoaderImpl: DbLoaderImpl): Boolean {
        return dbLoaderImpl.loadDelayedToManyRel(this)
    }
}
