package com.github.mslenc.dbktx.util

import kotlin.coroutines.Continuation

class PendingNotifications<T>(val result: T, val waiters: ListEl<Continuation<T>>) {
}