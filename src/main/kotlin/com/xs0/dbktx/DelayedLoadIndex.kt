package com.xs0.dbktx

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import mu.KLogging

import java.util.*
import kotlin.collections.HashSet

internal class DelayedLoadIndex<ENTITY, ID, META>(val metainfo: META) {

    private var index: HashMap<ID, DelayedLoadState<ENTITY>> = HashMap()
    private var idsToLoad = HashSet<ID>()

    operator fun get(id: ID): DelayedLoadState<ENTITY> {
        return index.computeIfAbsent(id) { i -> DelayedLoadState() }
    }

    fun addIdToLoad(id: ID) {
        idsToLoad.add(id)
    }

    fun getAndClearIdsToLoad(): MutableSet<ID> {
        val res = idsToLoad
        idsToLoad = HashSet()
        return res
    }

    fun reportEvent(ids: Set<ID>, errorEvent: AsyncResult<ENTITY>) {
        for (id in ids) {
            index[id]?.handleResult(errorEvent)
        }
    }

    fun flushAll(cleanUp: ArrayList<()->Unit>) {
        // if there are any outstanding IDs to load, we will fail them - obviously,
        // they either got confused about timing of stuff, or decided that there's no
        // point in waiting for something..

        val index = this.index
        this.index = HashMap()

        val idsToLoad = this.idsToLoad
        this.idsToLoad = HashSet()

        if (idsToLoad.isEmpty())
            return

        cleanUp.add({
            val error = Future.failedFuture<ENTITY>("The cache has been flushed")
            for (id in idsToLoad) {
                try {
                    index[id]?.handleResult(error)
                } catch (e: Exception) {
                    logger.error("Handler error when flushing cache", e)
                    // but continue anyway..
                }

            }
        })
    }

    companion object : KLogging()

    fun reportNull(ids: Set<ID>) {
        for (id in ids)
            index[id]?.handleResult(null)
    }
}
