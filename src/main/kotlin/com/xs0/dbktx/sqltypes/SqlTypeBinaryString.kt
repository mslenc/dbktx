package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.FieldProps
import com.xs0.dbktx.SqlBuilder

import java.util.Base64

import java.nio.charset.StandardCharsets.ISO_8859_1
import kotlin.reflect.KClass

class SqlTypeBinaryString(concreteType: SqlTypeKind, size: Int?, fieldProps: FieldProps) : SqlType<String>(fieldProps) {
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

    override fun fromJson(value: Any): String {
        if (value is CharSequence) {
            val encoded = value.toString()
            val bytes = Base64.getDecoder().decode(encoded)
            return String(bytes, ISO_8859_1)
        }

        throw IllegalArgumentException("Not a string(binary) value - " + value)
    }

    override fun toJson(value: String): String {
        val bytes = value.toByteArray(ISO_8859_1)
        return Base64.getEncoder().encodeToString(bytes)
    }

    override fun dummyValue(): String {
        var string = "strbin"
        if (string.length > maxSize)
            string = string.substring(0, maxSize)
        return string
    }

    override fun toSql(value: String, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql(toHexString(value.toByteArray(ISO_8859_1), "X'", "'"))
    }

    override val kotlinType: KClass<String> = String::class
}

private val HEX_CHARS = "0123456789ABCDEF".toCharArray()

internal fun toHexString(bytes: ByteArray): String {
    return toHexString(bytes, "", "")
}

internal fun toHexString(bytes: ByteArray, prefix: String, suffix: String): String {
    if (bytes.isEmpty())
        return if (prefix.isEmpty()) suffix else prefix + suffix

    val sb = StringBuilder(bytes.size * 2 + prefix.length + suffix.length)
    sb.append(prefix)

    for (b in bytes) {
        sb.append(HEX_CHARS[b.toInt() shr 4 and 15])
        sb.append(HEX_CHARS[b.toInt()       and 15])
    }

    sb.append(suffix)

    return sb.toString()
}
