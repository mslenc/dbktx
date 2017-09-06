package com.xs0.dbktx

import mu.KLogging

internal class MasterIndex {

    private val tableIndex = LinkedHashMap<DbTable<*,*>, DelayedLoadIndex<*,*,*>>()
    private val relToManyIndex = LinkedHashMap<RelToManyImpl<*,*,*,*>, DelayedLoadIndex<*,*,*>>()

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
    operator fun <E : DbEntity<E, ID>, ID: Any>
    get(table: DbTable<E, ID>): DelayedLoadIndex<E, ID, DbTable<E, ID>> {
        return tableIndex.computeIfAbsent(table,
                { _ -> DelayedLoadIndex<E, ID, DbTable<E, ID>>(table) })
                as DelayedLoadIndex<E, ID, DbTable<E, ID>>
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <FROM : DbEntity<FROM, FROMID>, FROMID: Any, TO : DbEntity<TO, TOID>, TOID: Any>
    get(rel: RelToMany<FROM, TO>): DelayedLoadIndex<List<TO>, FROMID, RelToManyImpl<FROM, FROMID, TO, TOID>> {
        rel as RelToManyImpl<FROM, FROMID, TO, TOID>

        return relToManyIndex.computeIfAbsent(rel,
                { _ -> DelayedLoadIndex<List<TO>, FROMID, RelToManyImpl<FROM, FROMID, TO, TOID>>(rel) })
                as DelayedLoadIndex<List<TO>, FROMID, RelToManyImpl<FROM, FROMID, TO, TOID>>
    }

    val allCachedTables: Collection<DelayedLoadIndex<*,*,*>>
        get() = tableIndex.values

    val allCachedToManyRels: Collection<DelayedLoadIndex<*,*,*>>
        get() = relToManyIndex.values

    companion object : KLogging()
}