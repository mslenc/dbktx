package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueByteArray
import com.github.mslenc.dbktx.util.Sql

import java.util.Base64

import java.nio.charset.StandardCharsets.ISO_8859_1
import kotlin.reflect.KClass

// VARBINARY, BLOB, etc => byte[]
class SqlTypeBlob(concreteType: SqlTypeKind, size: Int, isNotNull: Boolean) : SqlType<ByteArray>(isNotNull = isNotNull) {
    private val maxSize: Int

    init {
        when (concreteType) {
            SqlTypeKind.TINYBLOB -> this.maxSize = 255

            SqlTypeKind.BLOB -> this.maxSize = 65535

            SqlTypeKind.MEDIUMBLOB -> this.maxSize = 16777215

            SqlTypeKind.LONGBLOB -> this.maxSize = Integer.MAX_VALUE // it's actual twice that, but can't have an array that long in Java

            SqlTypeKind.BINARY -> {
                if (size > 255)
                    throw IllegalArgumentException("Invalid size, can be at most 255")
                this.maxSize = size
            }

            SqlTypeKind.VARBINARY -> {
                if (size > 65535)
                    throw IllegalArgumentException("Invalid size, can be at most 65535")
                this.maxSize = size
            }

            else -> throw IllegalArgumentException("Unsupported type $concreteType")
        }

        if (maxSize < 1)
            throw IllegalArgumentException("Invalid size, must be at least 1")
    }

    override fun parseDbValue(value: DbValue): ByteArray {
        return value.asByteArray()
    }

    override fun makeDbValue(value: ByteArray): DbValue {
        return DbValueByteArray(value)
    }

    override fun decodeFromJson(value: Any): ByteArray {
        if (value is CharSequence) {
            return Base64.getDecoder().decode(value.toString())
        } else {
            throw IllegalArgumentException("Not a string(base64) value - $value")
        }
    }

    override fun encodeForJson(value: ByteArray): Any {
        return Base64.getEncoder().encodeToString(value)
    }

    override fun toSql(value: ByteArray, sql: Sql) {
        sql(value)
    }

    override val zeroValue: ByteArray = byteArrayOf()

    override val kotlinType: KClass<ByteArray> = ByteArray::class
}