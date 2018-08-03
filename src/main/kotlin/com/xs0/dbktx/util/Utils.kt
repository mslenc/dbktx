package com.xs0.dbktx.util

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import kotlinx.coroutines.experimental.*
import java.lang.Long.reverseBytes
import kotlin.coroutines.experimental.suspendCoroutine
import java.util.regex.Pattern

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
    val a = extractIntBE(bytes, pos).toLong() and 0xFFFFFFFFL
    val b = extractIntBE(bytes, pos + 4).toLong() and 0xFFFFFFFFL

    return a.shl(32) or b
}

fun putIntBE(value: Int, bytes: ByteArray, pos: Int) {
    bytes[pos    ] = value.ushr(24).toByte()
    bytes[pos + 1] = value.ushr(16).toByte()
    bytes[pos + 2] = value.ushr( 8).toByte()
    bytes[pos + 3] = value         .toByte()
}

fun putIntLE(value: Int, bytes: ByteArray, pos: Int) {
    bytes[pos    ] = value         .toByte()
    bytes[pos + 1] = value.ushr( 8).toByte()
    bytes[pos + 2] = value.ushr(16).toByte()
    bytes[pos + 3] = value.ushr(24).toByte()
}

fun putLongBE(value: Long, bytes: ByteArray, pos: Int) {
    putIntBE(value.ushr(32).toInt(), bytes, pos)
    putIntBE(value.toInt(), bytes, pos + 4)
}

fun putLongLE(value: Long, bytes: ByteArray, pos: Int) {
    putLongBE(reverseBytes(value), bytes, pos)
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



fun escapeSqlLikePattern(pattern: String, escapeChar: Char): String {
    // lazy allocate, because most patterns are not expected to actually contain
    // _ or %
    var sb: StringBuilder? = null

    var i = 0
    val n = pattern.length
    while (i < n) {
        val c = pattern[i]
        if (c == '%' || c == '_' || c == escapeChar) {
            if (sb == null) {
                sb = StringBuilder(pattern.length + 16)
                sb.append(pattern, 0, i)
            }
            sb.append(escapeChar)
        }
        if (sb != null)
            sb.append(c)
        i++
    }

    return if (sb != null) sb.toString() else pattern
}

inline fun <T, K> List<T>.groupBy(selector: (T)->K): MutableMap<K, MutableList<T>> {
    val result: MutableMap<K, MutableList<T>> = LinkedHashMap()
    for (el in this) {
        val key = selector(el)
        result.computeIfAbsent(key, { _ -> ArrayList() }).add(el)
    }
    return result
}

inline fun <T, K> List<T>.groupByNullable(selector: (T)->K?): MutableMap<K?, MutableList<T>> {
    val result: MutableMap<K?, MutableList<T>> = LinkedHashMap()
    for (el in this) {
        val key = selector(el)
        result.computeIfAbsent(key, { _ -> ArrayList() }).add(el)
    }
    return result
}

inline fun <T, K> List<T>.indexBy(selector: (T)->K): MutableMap<K, T> {
    val result: MutableMap<K, T> = LinkedHashMap()
    for (el in this) {
        val key = selector(el)
        if (result.put(key, el) != null)
            throw IllegalStateException("More than one element mapped to $key")
    }
    return result
}

inline fun <T> MutableList<T>.removeFirstMatching(selector: (T)->Boolean): Boolean {
    for (i in 0 until size) {
        if (selector(get(i))) {
            removeAt(i)
            return true
        }
    }
    return false
}

inline fun <T> List<T>.indexOfFirstMatching(selector: (T)->Boolean): Int? {
    for (i in 0 until size) {
        if (selector(get(i))) {
            return i
        }
    }
    return null
}

fun String?.trimToNull(): String? {
    if (this == null)
        return null

    val trimmed = this.trim()
    if (trimmed.isEmpty())
        return null

    return trimmed
}

private val wordPattern: Pattern = Pattern.compile("\\b\\w+\\b", Pattern.CASE_INSENSITIVE or Pattern.UNICODE_CHARACTER_CLASS or Pattern.UNICODE_CASE)

fun extractWordsForSearch(query: String): List<String> {
    val matcher = wordPattern.matcher(query)
    val result = ArrayList<String>()
    while (matcher.find()) {
        val word = matcher.group().trim { !it.isLetterOrDigit() }
        if (word.isNotEmpty())
            result.add(word)
    }
    return result
}