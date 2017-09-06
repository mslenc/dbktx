package com.xs0.dbktx

interface DelayedExecScheduler {
    fun schedule(runnable: () -> Unit)
}