package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.utils.error
import com.github.mslenc.utils.getLogger
import com.github.mslenc.utils.trace
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlin.coroutines.*

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

enum class BatchKeyState {
    INITIAL,    // the data is not ready
    COMPUTING,  // the loader is executing right now
    LOADED      // the value is loaded
}

class BatchValueState<RES> {
    private var existingResult: RES? = null // only if state == LOADED
    private var handlers: ListEl<Continuation<RES>>? = null // only if state != LOADED
    private var parentBatches: MutableSet<ComputeBatch>? = null // only if state != LOADED
    var state = BatchKeyState.INITIAL
        private set

    fun storeResult(result: RES): ListEl<Continuation<RES>>? {
        return when (state) {
            BatchKeyState.INITIAL,       // <-- we seem to have got loaded as a side effect of something else; we just cache the result
            BatchKeyState.COMPUTING -> { // <-- the normal case, our computation finished
                state = BatchKeyState.LOADED
                existingResult = result
                val handlers = this.handlers
                this.handlers = null
                this.parentBatches = null
                handlers
            }

            BatchKeyState.LOADED -> {
                // no-op - we were already loaded, now we're still loaded..
                null
            }
        }
    }

    fun handleError(): ListEl<Continuation<RES>>? {
        when (state) {
            BatchKeyState.INITIAL -> {
                // we stay INITIAL, so that we might try again (the error could be unrelated to this guy)
                return null
            }

            BatchKeyState.COMPUTING -> {
                state = BatchKeyState.INITIAL
                val handlers = this.handlers
                this.handlers = null
                this.parentBatches = null
                return handlers
            }

            BatchKeyState.LOADED -> {
                // we ignore the error as we were successfully loaded already
                return null
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    val value: RES
        get() {
            if (state == BatchKeyState.LOADED)
                return existingResult as RES

            throw IllegalStateException("get() called when not LOADED")
        }

    fun addReceiver(cont: Continuation<RES>, parentBatches: ComputeBatches?) {
        handlers = ListEl(cont, handlers)
        if (parentBatches != null) {
            if (this.parentBatches == null) {
                this.parentBatches = parentBatches.batches.toMutableSet()
            } else {
                this.parentBatches!!.addAll(parentBatches.batches)
            }
        }
    }

    fun startedComputing(outBatches: MutableSet<ComputeBatch>): Boolean {
        return when (state) {
            BatchKeyState.INITIAL -> {
                state = BatchKeyState.COMPUTING
                this.parentBatches?.let { outBatches.addAll(it) }
                true
            }
            else -> {
                false
            }
        }
    }
}

class ComputeBatch(
    val loader: BatchingLoader<*, *>,
    val keys: Set<*>
)

class ComputeBatches(
    val batches: Set<ComputeBatch>
) : AbstractCoroutineContextElement(ComputeBatches) {
    companion object Key : CoroutineContext.Key<ComputeBatches>
}



internal class BatchingLoaderIndex<KEY: Any, RESULT>(val loader: BatchingLoader<KEY, RESULT>) {
    private var scheduled = false
    private var index: HashMap<KEY, BatchValueState<RESULT>> = HashMap()
    private var keysToLoad = LinkedHashSet<KEY>()

    private fun ensureScheduled(db: DbConn, scope: CoroutineScope) {
        if (!scheduled) {
            logger.trace { "Scheduling $loader for execution" }
            scheduled = true
            scope.launch {
                scheduled = false
                performDelayedOps(db)
            }
        }
    }

    suspend fun provideValue(key: KEY, db: DbConn, scope: CoroutineScope): RESULT {
        val valueState = index.computeIfAbsent(key) { BatchValueState() }

        val parentBatches = coroutineContext[ComputeBatches]

        return when (valueState.state) {
            BatchKeyState.INITIAL -> {
                logger.trace { "Adding key $key of loader $loader to list for loading and scheduling" }
                keysToLoad.add(key)
                ensureScheduled(db, scope)
                suspendCoroutine { valueState.addReceiver(it, parentBatches) }
            }

            BatchKeyState.COMPUTING -> {
                parentBatches?.let {
                    for (batch in it.batches) {
                        if (batch.loader == loader && batch.keys.contains(key)) {
                            logger.error { "Circular custom loader $loader detected with key $key" }
                            throw IllegalStateException("Circular custom loader $loader detected with key $key")
                        }
                    }
                }
                suspendCoroutine { valueState.addReceiver(it, parentBatches) }
            }

            BatchKeyState.LOADED -> {
                logger.trace { "Key $key of loader $loader was already loaded" }
                valueState.value
            }
        }
    }

    private suspend fun performDelayedOps(db: DbConn) {
        logger.trace { "Started loading from $loader" }
        val keys = this.keysToLoad
        if (keys.isEmpty())
            return

        this.keysToLoad = LinkedHashSet()

        val parentBatches = HashSet<ComputeBatch>()

        val it = keys.iterator()
        while (it.hasNext()) {
            val key = it.next()
            val valueState = index[key]
            if (valueState == null || !valueState.startedComputing(parentBatches)) {
                it.remove()
            }
        }

        if (keys.isEmpty())
            return

        parentBatches.add(ComputeBatch(loader, keys))

        val result = try {
            logger.trace { "Starting $loader computation with ${parentBatches.size} parent batches for keys $keys " }
            try {
                withContext(ComputeBatches(parentBatches)) {
                    loader.loadNow(keys, db)
                }
            } finally {
                logger.trace { "Ended $loader computation with ${parentBatches.size} parent batches for keys $keys " }
            }
        } catch (e: Throwable) {
            reportError(keys, e)
            return
        }

        val toNotify = ArrayList<PendingNotifications<RESULT>>()

        for ((key, value) in result) {
            val valueState = index.computeIfAbsent(key) { BatchValueState() }
            val waiters = valueState.storeResult(value)
            waiters?.let { toNotify.add(PendingNotifications(value, it)) }
            keys.remove(key)
        }

        if (keys.isNotEmpty()) run {
            logger.trace { "Loader $loader had unhandled keys $keys" }
            val nullResult = try {
                loader.nullResult()
            } catch (e: Throwable) {
                logger.error(e) { "Couldn't obtain null result from loader $loader" }
                reportError(keys, e)
                return@run
            }

            for (key in keys) {
                val valueState = index[key] ?: continue
                val waiters = valueState.storeResult(nullResult)
                waiters?.let { toNotify.add(PendingNotifications(nullResult, it)) }
            }
        }

        logger.trace { "Starting notifications for $loader - ${ toNotify.size } waiter lists"}
        try {
            for (notification in toNotify) {
                notifyCont(notification.waiters, notification.result)
            }
        } finally {
            logger.trace { "Notifications for $loader finished" }
        }
    }

    fun reportError(ids: Set<KEY>, error: Throwable) {
        for (id in ids) {
            index[id]?.let { valueState ->
                valueState.handleError()?.let { waiters ->
                    notifyError(waiters, error)
                }
            }
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
                    index[key]?.handleError()?.let { waiters ->
                        notifyError(waiters, error)
                    }
                } catch (e: Throwable) {
                    logger.error("Handler error when flushing cache", e)
                    // but continue anyway..
                }
            }
        }
    }

    companion object {
        val logger = getLogger<BatchingLoaderIndex<*,*>>()
    }
}
