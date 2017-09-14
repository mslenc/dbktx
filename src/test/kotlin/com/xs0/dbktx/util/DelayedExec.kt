package com.xs0.dbktx.util

import java.util.ArrayList

class DelayedExec : DelayedExecScheduler {
    private val pending = ArrayList<() -> Unit>()

    override fun schedule(runnable: () -> Unit) {
        pending.add(runnable)
    }

    fun executePending() {
        while (!pending.isEmpty())
            pending.removeAt(0)()
    }
}
