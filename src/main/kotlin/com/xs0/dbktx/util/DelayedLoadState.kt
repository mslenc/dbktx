package com.xs0.dbktx.util

import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.launch
import mu.KotlinLogging
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.suspendCoroutine

private val logger = KotlinLogging.logger {}

internal class DelayedLoadState<RES> {
    private var existingResult: RES? = null // only if state == LOADED
    internal var state = EntityState.INITIAL
        private set
    private var handlers: ListEl<Continuation<RES>>? = null // only if state == LOADING

    fun handleResult(result: RES) {
        when (state) {
            EntityState.INITIAL -> {
                state = EntityState.LOADED
                existingResult = result
            }

            EntityState.LOADING -> {
                state = EntityState.LOADED
                existingResult = result
                val handlers = this.handlers
                this.handlers = null
                notifyCont(handlers, result)
            }

            EntityState.LOADED -> { // already done, so no-op
            }
        }
    }

    fun handleError(error: Throwable) {
        when (state) {
            EntityState.INITIAL -> {
                // we stay INITIAL, so that we might try again (the error could be unrelated to this guy)
            }

            EntityState.LOADING -> {
                state = EntityState.INITIAL
                val handlers = this.handlers
                this.handlers = null
                notifyError(handlers, error)
            }

            EntityState.LOADED -> {
                // we ignore the error as we were successfully loaded already
            }
        }
    }

    val value: RES
        get() {
            if (state == EntityState.LOADED)
                return existingResult!!

            throw IllegalStateException("get() called when not LOADED")
        }

    fun addReceiver(cont: Continuation<RES>) {
        handlers = ListEl(cont, handlers)
    }

    fun startedLoading(cont: Continuation<RES>) {
        if (state != EntityState.INITIAL)
            throw IllegalStateException("startedLoading() called, but state is not INITIAL")

        state = EntityState.LOADING
        handlers = ListEl(cont, handlers)
    }

    suspend fun startLoading(resProvider: suspend () -> RES): RES {
        return suspendCoroutine { cont ->
            startedLoading(cont)

            launch(Unconfined) {
                val result: RES
                try {
                    result = resProvider()
                } catch (t: Throwable) {
                    handleError(t)
                    return@launch
                }

                handleResult(result)
            }
        }
    }
}

internal class DelayedLoadStateNullable<RES> {
    private var existingResult: RES? = null // only if state == LOADED
    internal var state = EntityState.INITIAL
        private set
    private var handlers: ListEl<Continuation<RES?>>? = null // only if state == LOADING

    fun handleResult(result: RES?) {
        when (state) {
            EntityState.INITIAL -> {
                state = EntityState.LOADED
                existingResult = result
            }

            EntityState.LOADING -> {
                state = EntityState.LOADED
                existingResult = result
                val handlers = this.handlers
                this.handlers = null
                notifyContNullable(handlers, result)
            }

            EntityState.LOADED -> { // already done, so no-op
            }
        }
    }

    fun handleError(error: Throwable) {
        when (state) {
            EntityState.INITIAL -> {
                // we stay INITIAL, so that we might try again (the error could be unrelated to this guy)
            }

            EntityState.LOADING -> {
                state = EntityState.INITIAL
                val handlers = this.handlers
                this.handlers = null
                notifyError(handlers, error)
            }

            EntityState.LOADED -> {
                // we ignore the error as we were successfully loaded already
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

    suspend fun startLoading(resProvider: suspend () -> RES?): RES? {
        return suspendCoroutine { cont ->
            startedLoading(cont)

            launch(Unconfined) {
                val result: RES?
                try {
                    result = resProvider()
                } catch (t: Throwable) {
                    handleError(t)
                    return@launch
                }

                handleResult(result)
            }
        }
    }

    fun replaceResult(result: RES) {
        if (state != EntityState.LOADED)
            throw IllegalStateException("Can only replace result when state == LOADED")

        existingResult = result
    }
}


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