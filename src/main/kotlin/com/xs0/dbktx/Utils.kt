package com.xs0.dbktx

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import kotlinx.coroutines.experimental.*
import mu.KotlinLogging
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.suspendCoroutine

private val logger = KotlinLogging.logger {}

fun <T> makeHandler(onError: (Throwable) -> Unit, onSuccess: (T) -> Unit): Handler<AsyncResult<T>> {
    return Handler { result ->
        if (result.succeeded()) {
            try {
                onSuccess(result.result())
            } catch (t: Throwable) {
                onError(t)
            }

        } else {
            onError(result.cause())
        }
    }
}

private object HexUtil {
    val reverseHex: IntArray = kotlin.IntArray(256).apply {
        fill(-1)

        for (a in 0..9)
            this['0'.toInt() + a] = a

        for (a in 0..5) {
            this['a'.toInt() + a] = 10 + a
            this['A'.toInt() + a] = 10 + a
        }
    }
}


fun bytesFromHex(str: ByteArray): ByteArray {
    val inLen = str.size
    val outLen = inLen / 2

    if (inLen.rem(2) != 0)
        throw IllegalArgumentException("Length not divisible by 2")

    val out = ByteArray(outLen)

    var outPos = 0
    for (inPos in 0 until inLen step 2) {
        val b0 = HexUtil.reverseHex[str[inPos    ].toInt() and 255]
        val b1 = HexUtil.reverseHex[str[inPos + 1].toInt() and 255]

        if (b0 < 0 || b1 < 0)
            throw IllegalArgumentException("Not a hex string")

        out[outPos++] = (b0 shl 4 or b1).toByte()
    }

    return out
}

private val HEX_CHARS = "0123456789ABCDEF".toCharArray()

fun toHexBytes(bytes: ByteArray): ByteArray {
    if (bytes.isEmpty())
        return bytes

    val res = ByteArray(bytes.size * 2)
    var pos = 0

    for (b in bytes) {
        res[pos++] = HEX_CHARS[b.toInt().shr(4).and(15)].toByte()
        res[pos++] = HEX_CHARS[b.toInt()       .and(15)].toByte()
    }

    return res
}

fun bytesFromHex(str: String): ByteArray {
    val inLen = str.length
    val outLen = inLen / 2
    if (inLen.rem(2) != 0)
        throw IllegalArgumentException("Length not divisible by 2")

    val out = ByteArray(outLen)

    var outPos = 0
    for (inPos in 0 until inLen step 2) {
        val char0 = str[inPos    ].toInt()
        val char1 = str[inPos + 1].toInt()

        if (minOf(char0, char1) < 0 || maxOf(char0, char1) > 255)
            throw IllegalArgumentException("Not a hex string")

        val b0: Int = HexUtil.reverseHex[char0]
        val b1: Int = HexUtil.reverseHex[char1]

        if (b0 < 0 || b1 < 0)
            throw IllegalArgumentException("Not a hex string")

        out[outPos++] = (b0 * 16 + b1).toByte()
    }

    return out
}

fun extractIntBE(bytes: ByteArray, pos: Int): Int {
    val a = bytes[pos    ].toInt() and 255
    val b = bytes[pos + 1].toInt() and 255
    val c = bytes[pos + 2].toInt() and 255
    val d = bytes[pos + 3].toInt() and 255

    return a.shl(24) or b.shl(16) or c.shl(8) or d
}

fun extractLongBE(bytes: ByteArray, pos: Int): Long {
    val a = extractIntBE(bytes, pos    ).toLong() and 0xFFFFFFFFL
    val b = extractIntBE(bytes, pos + 4).toLong() and 0xFFFFFFFFL

    return a.shl(32) or b
}

fun putIntBE(value: Int, bytes: ByteArray, pos: Int) {
    bytes[pos    ] = value.ushr(24).toByte()
    bytes[pos + 1] = value.ushr(16).toByte()
    bytes[pos + 2] = value.ushr( 8).toByte()
    bytes[pos + 3] = value         .toByte()
}

fun putLongBE(value: Long, bytes: ByteArray, pos: Int) {
    putIntBE(value.ushr(32).toInt(), bytes, pos    )
    putIntBE(value         .toInt(), bytes, pos + 4)
}

fun <T> defer(block: suspend () -> T): Deferred<T> {
    return async(Unconfined) { block() }
}

inline suspend fun <T> vx(crossinline callback: (Handler<AsyncResult<T>>) -> Unit) =
        suspendCoroutine<T> { cont ->
            callback(Handler { result: AsyncResult<T> ->
                if (result.succeeded()) {
                    cont.resume(result.result())
                } else {
                    cont.resumeWithException(result.cause())
                }
            })
        }

fun <T> notifyCont(handlerList: ListEl<Continuation<T?>>?, result: T?) {
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