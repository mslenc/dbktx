package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.conn.DbConn
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.asContextElement

private val threadLocal = ThreadLocal<DbConn>()

fun makeDbContext(db: DbConn): ThreadContextElement<DbConn> {
    return threadLocal.asContextElement(db)
}

fun getContextDb(): DbConn {
    return threadLocal.get() ?: throw IllegalStateException("No db in coroutine context")
}
