package com.github.mslenc.dbktx.util

import kotlin.coroutines.Continuation
import kotlin.coroutines.suspendCoroutine

class CachedAsync<T: Any>(private val compute: suspend ()->T) {
    private var state = INITIAL
    private lateinit var value: T
    private lateinit var error: Throwable
    private var waiters: MutableList<Continuation<T>>? = null

    suspend fun get(): T {
        when (state) {
            INITIAL -> {
                state = COMPUTING

                try {
                    value = compute()
                    state = COMPUTED
                    wakeUp(Result.success(value))
                    return value
                } catch (t: Throwable) {
                    error = t
                    state = FAILED
                    wakeUp(Result.failure(t))
                    throw t
                }
            }

            COMPUTING -> {
                return suspendCoroutine { cont ->
                    waiters.let { existingList ->
                        if (existingList != null) {
                            existingList.add(cont)
                        } else {
                            with(ArrayList<Continuation<T>>()) {
                                add(cont)
                                waiters = this
                            }
                        }
                    }
                }
            }

            COMPUTED -> {
                return value
            }

            else -> {
                throw error
            }
        }
    }

    private fun wakeUp(result: Result<T>) = waiters.let { waiters ->
        if (waiters == null)
            return

        this.waiters = null

        for (cont in waiters) {
            cont.resumeWith(result)
        }
    }

    companion object {
        private const val INITIAL = 0
        private const val COMPUTING = 1
        private const val COMPUTED = 2
        private const val FAILED = 3
    }
}