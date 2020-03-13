package com.github.mslenc.dbktx.util

import com.github.mslenc.utils.getLogger
import com.github.mslenc.utils.trace
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

internal class DelayedLoadStateNullable<RES> {
    private var existingResult: RES? = null // only if state == LOADED
    internal var state = EntityState.INITIAL
        private set
    private var handlers: ListEl<Continuation<RES?>>? = null // only if state == LOADING

    fun handleResult(result: RES?) {
        when (state) {
            EntityState.INITIAL -> {
                logger.trace { "Result received in INITIAL state: $result" }
                state = EntityState.LOADED
                existingResult = result
            }

            EntityState.LOADING -> {
                logger.trace { "Result received in LOADING state: $result" }
                state = EntityState.LOADED
                existingResult = result
                val handlers = this.handlers
                this.handlers = null
                notifyContNullable(handlers, result)
            }

            EntityState.LOADED -> { // already done, so no-op
                logger.trace { "Result received in LOADED state: $result" }
            }
        }
    }

    fun handleError(error: Throwable) {
        when (state) {
            EntityState.INITIAL -> {
                // we stay INITIAL, so that we might try again (the error could be unrelated to this guy)
                logger.trace { "Error received in LOADED state" }
            }

            EntityState.LOADING -> {
                logger.trace { "Error received in LOADING state" }
                state = EntityState.INITIAL
                val handlers = this.handlers
                this.handlers = null
                notifyError(handlers, error)
            }

            EntityState.LOADED -> {
                // we ignore the error as we were successfully loaded already
                logger.trace { "Error received in LOADED state" }
            }
        }
    }

    val value: RES?
        get() {
            if (state == EntityState.LOADED)
                return existingResult

            throw IllegalStateException("get() called when not LOADED")
        }

    fun addReceiver(cont: Continuation<RES?>) {
        handlers = ListEl(cont, handlers)
    }

    fun startedLoading(cont: Continuation<RES?>) {
        if (state != EntityState.INITIAL)
            throw IllegalStateException("startedLoading() called, but state is not INITIAL")

        state = EntityState.LOADING
        handlers = ListEl(cont, handlers)
    }

    fun replaceResult(result: RES) {
        if (state != EntityState.LOADED)
            throw IllegalStateException("Can only replace result when state == LOADED")

        existingResult = result
    }
}

val logger = getLogger<DelayedLoadStateNullable<*>>()

fun <T> notifyError(handlerList: ListEl<Continuation<T>>?, error: Throwable) {
    var handlers = handlerList

    while (handlers != null) {
        val curr = handlers.value
        handlers = handlers.next

        try {
            curr.resumeWithException(error)
        } catch (t: Throwable) {
            logger.error("Error while executing continuation", t)
        }
    }
}

internal fun <T> notifyCont(handlerList: ListEl<Continuation<T>>?, result: T) {
    var handlers = handlerList

    while (handlers != null) {
        val curr = handlers.value
        handlers = handlers.next

        try {
            curr.resume(result)
        } catch (t: Throwable) {
            logger.error("Error while executing continuation", t)
        }
    }
}

internal fun <T> notifyContNullable(handlerList: ListEl<Continuation<T?>>?, result: T?) {
    var handlers = handlerList

    while (handlers != null) {
        val curr = handlers.value
        handlers = handlers.next

        try {
            curr.resume(result)
        } catch (t: Throwable) {
            logger.error("Error while executing continuation", t)
        }
    }
}