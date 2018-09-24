package com.github.mslenc.dbktx.util

interface DelayedExecScheduler {
    fun schedule(runnable: () -> Unit)
}