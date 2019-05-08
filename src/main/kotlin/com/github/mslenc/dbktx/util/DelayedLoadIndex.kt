package com.github.mslenc.dbktx.util

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbLoaderInternal
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.dbktx.schema.RelToManyImpl
import com.github.mslenc.dbktx.schema.UniqueKeyDef
import kotlinx.coroutines.CoroutineScope
import mu.KLogging

import java.util.*
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashSet


internal class IndexAndKeys<E: DbEntity<E, *>, KEY: Any>(
    val index: SingleKeyIndex<E, KEY>,
    val keys: MutableSet<KEY>
)


internal class SingleKeyIndex<E: DbEntity<E, *>, KEY: Any>(
    val table: DbTable<E, *>,
    val keyDef: UniqueKeyDef<E, KEY>,
    val scope: CoroutineScope
) {
    private var index = HashMap<KEY, DelayedLoadStateNullable<E>>()
    private var keysToLoad = LinkedHashSet<KEY>()

    operator fun get(key: KEY): DelayedLoadStateNullable<E> {
        return index.computeIfAbsent(key) { DelayedLoadStateNullable(scope) }
    }

    fun addKeyToLoad(key: KEY) {
        keysToLoad.add(key)
    }

    fun removeIdToLoad(key: KEY): Boolean {
        return keysToLoad.remove(key)
    }

    fun getAndClearKeysToLoad(): IndexAndKeys<E, KEY>? {
        if (keysToLoad.isEmpty())
            return null

        val keys = keysToLoad
        keysToLoad = LinkedHashSet()

        return IndexAndKeys(this, keys)
    }

    fun reportNull(keys: Set<KEY>) {
        for (key in keys)
            index[key]?.handleResult(null)
    }

    fun reportError(keys: Set<KEY>, error: Throwable) {
        for (key in keys) {
            index[key]?.handleError(error)
        }
    }

    fun flushAll(cleanUp: ArrayList<()->Unit>) {
        // if there are any outstanding IDs to load, we will fail them - obviously,
        // they either got confused about timing of stuff, or decided that there's no
        // point in waiting for something..

        val index = this.index
        this.index = HashMap()

        val keysToLoad = this.keysToLoad
        this.keysToLoad = LinkedHashSet()

        if (keysToLoad.isEmpty())
            return

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
}

internal class EntityIndex<E : DbEntity<E, *>>(val metainfo: DbTable<E, *>, val scope: CoroutineScope) {

    val indexes = Array<SingleKeyIndex<E, *>>(metainfo.uniqueKeys.size) { SingleKeyIndex(metainfo, metainfo.uniqueKeys[it], scope) }

    @Suppress("UNCHECKED_CAST")
    fun <T: Any> getSingleKeyIndex(keyDef: UniqueKeyDef<E, T>): SingleKeyIndex<E, T> {
        return indexes[keyDef.indexInTable] as SingleKeyIndex<E, T>
    }

    fun getAndClearKeysToLoad(): IndexAndKeys<E, *>? {
        for (index in indexes) {
            val res = index.getAndClearKeysToLoad()
            if (res != null) {
                return res
            }
        }

        return null
    }

    fun flushAll(cleanUp: ArrayList<()->Unit>) {
        indexes.forEach { it.flushAll(cleanUp) }
    }

    companion object : KLogging()

    internal suspend fun loadNow(dbLoaderImpl: DbLoaderInternal): Boolean {
        return dbLoaderImpl.loadDelayedTable(this)
    }

    fun rowLoaded(db: DbConn, row: DbRow, selectForUpdate: Boolean): E {
        // first, we create/reuse the entity; then we go through all indexes and insert the entity in them..

        val entity: E = metainfo.callInsertAndResolveEntityInIndex(this, db, row, selectForUpdate)

        for (i in 1 until indexes.size) {
            val index = indexes[i]
            insertEntityInIndex(index, entity)
        }

        return entity
    }

    internal fun <Z: DbEntity<Z, T>, T: Any>
    insertAndResolveEntityInIndex(db: DbConn, metainfo: DbTable<Z, T>, row: DbRow, selectForUpdate: Boolean): Z {
        @Suppress("UNCHECKED_CAST")
        val primaryIndex = indexes[0] as SingleKeyIndex<Z, T>
        val primaryId: T = primaryIndex.keyDef.invoke(row)
        val primaryInfo = primaryIndex[primaryId]

        val entity: Z
        if (primaryInfo.state == EntityState.LOADED) {
            val entityMaybe = primaryInfo.value
            if (entityMaybe != null && !selectForUpdate) {
                entity = entityMaybe
            } else {
                entity = metainfo.create(db, primaryId, row)
                primaryInfo.replaceResult(entity)
            }
        } else {
            entity = metainfo.create(db, primaryId, row)
            primaryInfo.handleResult(entity)
            primaryIndex.removeIdToLoad(primaryId)
        }

        return entity
    }

    private fun <T: Any>
    insertEntityInIndex(index: SingleKeyIndex<E, T>, entity: E) {
        val key = index.keyDef.invoke(entity)
        index.removeIdToLoad(key)
        index[key].handleResult(entity)
    }
}

