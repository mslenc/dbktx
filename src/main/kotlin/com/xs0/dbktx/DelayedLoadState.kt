package com.xs0.dbktx

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import kotlin.coroutines.experimental.Continuation

internal class DelayedLoadState<RES> {
    private var existingResult: RES? = null // only if state == LOADED
    private var state = EntityState.INITIAL
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

    /**
     * Returns true if loading needs to start.
     */
    fun addHandler(handler: Handler<AsyncResult<RES>>): Boolean {
        val startLoading: Boolean
        val alreadyLoaded: AsyncResult<RES>?

        when (state) {
            EntityState.INITIAL -> {
                startLoading = true
                alreadyLoaded = null
                state = EntityState.LOADING
            }

            EntityState.LOADING -> {
                startLoading = false
                alreadyLoaded = null
            }

            EntityState.LOADED -> {
                startLoading = false
                alreadyLoaded = existingResult
            }
        }

        if (alreadyLoaded != null) {
            handler.handle(alreadyLoaded)
        } else {
            this.handlers = ListEl(handler, this.handlers)
        }

        return startLoading
    }

    fun replaceResult(result: AsyncResult<RES>) {
        this.existingResult = result
    }

    val loaded: Boolean
        get() {
            return state == EntityState.LOADED
        }

    val loading: Boolean
        get() {
            return state == EntityState.LOADING
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
}