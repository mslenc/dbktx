package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueByteArray
import com.github.mslenc.dbktx.util.Sql
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

import kotlin.reflect.KClass

class SqlTypeBinaryString(concreteType: SqlTypeKind, size: Int?, isNotNull: Boolean, private val charset: Charset = StandardCharsets.ISO_8859_1) : SqlType<String>(isNotNull, false) {
    private val maxSize: Int

    init {
        when (concreteType) {
            SqlTypeKind.TINYBLOB -> this.maxSize = 255

            SqlTypeKind.BLOB -> this.maxSize = 65535

            SqlTypeKind.MEDIUMBLOB -> this.maxSize = 16777215

            SqlTypeKind.LONGBLOB -> this.maxSize = Integer.MAX_VALUE // it's actual twice that, but can't have an array that long in Java

            SqlTypeKind.BINARY -> {
                if (size == null)
                    throw IllegalArgumentException("null size for BINARY")
                if (size > 255)
                    throw IllegalArgumentException("Invalid size, can be at most 255")
                this.maxSize = size
            }

            SqlTypeKind.VARBINARY -> {
                if (size == null)
                    throw IllegalArgumentException("null size for VARBINARY")
                if (size > 65535)
                    throw IllegalArgumentException("Invalid size, can be at most 65535")
                this.maxSize = size
            }

            else -> throw IllegalArgumentException("Unsupported type " + concreteType)
        }

        if (maxSize < 1)
            throw IllegalArgumentException("Invalid size, must be at least 1")
    }

    override fun parseDbValue(value: DbValue): String {
        return value.asByteArray().toString(charset)
    }

    override fun makeDbValue(value: String): DbValue {
        return DbValueByteArray(value.toByteArray(charset))
    }

    override fun encodeForJson(value: String): Any {
        return value
    }

    override fun decodeFromJson(value: Any): String {
        return value.toString()
    }

    override fun toSql(value: String, sql: Sql) {
        sql(value.toByteArray(charset))
    }

    override val zeroValue: String = ""

    override val kotlinType: KClass<String> = String::class
}

private val HEX_CHARS = "0123456789ABCDEF".toCharArray()

internal fun toHexString(bytes: ByteArray): String {
    return toHexString(bytes, "", "")
}

internal fun toHexString(bytes: ByteArray, prefix: String, suffix: String): String {
    val sb = StringBuilder(bytes.size * 2 + prefix.length + suffix.length)
    toHexString(bytes, prefix, suffix, sb)
    return sb.toString()
}

internal fun toHexString(bytes: ByteArray, prefix: String, suffix: String, out: StringBuilder) {
    out.append(prefix)

    for (b in bytes) {
        val i = b.toInt() and 255
        out.append(HEX_CHARS[i shr  4])
        out.append(HEX_CHARS[i and 15])
    }

    out.append(suffix)
}
