package com.xs0.dbktx.util

interface DelayedExecScheduler {
    fun schedule(runnable: () -> Unit)
}