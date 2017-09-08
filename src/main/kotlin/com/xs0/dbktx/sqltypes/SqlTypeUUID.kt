package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.*
import com.xs0.dbktx.util.*
import java.util.Base64
import java.util.UUID

import java.nio.charset.StandardCharsets.ISO_8859_1
import kotlin.reflect.KClass

abstract class SqlTypeUUID protected constructor(isNotNull: Boolean) : SqlType<UUID>(isNotNull = isNotNull) {
    override fun dummyValue(): UUID {
        return UUID.randomUUID()
    }

    override val kotlinType: KClass<UUID> = UUID::class


    private class VarcharRawChars(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {
        override fun fromJson(value: Any): UUID {
            return (value as String).toByteArray(ISO_8859_1).toUUID()
        }

        override fun toJson(value: UUID): String {
            return String(value.toBytes(), ISO_8859_1)
        }

        override fun toSql(value: UUID, sb: SqlBuilder, topLevel: Boolean) {
            sb.sql("?").param(toJson(value))
        }
    }

    private class VarcharBase64(isNotNull: Boolean, private val skipPadding: Boolean) : SqlTypeUUID(isNotNull) {

        override fun fromJson(value: Any): UUID {
            return Base64.getDecoder().decode(value as String).toUUID()
        }

        override fun toJson(value: UUID): String {
            val bytes = value.toBytes()
            return if (skipPadding) {
                Base64.getEncoder().withoutPadding().encodeToString(bytes)
            } else {
                Base64.getEncoder().encodeToString(bytes)
            }
        }

        override fun toSql(value: UUID, sb: SqlBuilder, topLevel: Boolean) {
            sb.sql("?").param(toJson(value))
        }
    }

    private class VarcharHex(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {
        override fun fromJson(value: Any): UUID {
            return bytesFromHex(value as String).toUUID()
        }

        override fun toJson(value: UUID): String {
            return toHexString(value.toBytes())
        }

        override fun toSql(value: UUID, sb: SqlBuilder, topLevel: Boolean) {
            sb.sql("?").param(toJson(value))
        }
    }

    private class VarcharFullString(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {

        override fun fromJson(value: Any): UUID {
            return UUID.fromString(value as String)
        }

        override fun toJson(value: UUID): String {
            return value.toString()
        }

        override fun toSql(value: UUID, sb: SqlBuilder, topLevel: Boolean) {
            sb.sql("?").param(value.toString())
        }
    }

    private class BinaryRawChars(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {

        override fun fromJson(value: Any): UUID {
            return Base64.getDecoder().decode(value as String).toUUID()
        }

        override fun toJson(value: UUID): String {
            return Base64.getEncoder().encodeToString(value.toBytes())
        }

        override fun toSql(value: UUID, sb: SqlBuilder, topLevel: Boolean) {
            sb.sql(toHexString(value.toBytes(), "X'", "'"))
        }
    }

    private class BinaryBase64(isNotNull: Boolean, private val skipPadding: Boolean) : SqlTypeUUID(isNotNull) {

        override fun fromJson(value: Any): UUID {
            // we have base64 in DB, and then again in the json thing (due to being binary)
            val base64 = Base64.getDecoder().decode(value as String)
            return Base64.getDecoder().decode(base64).toUUID()
        }

        override fun toJson(value: UUID): String {
            val bytes = value.toBytes()
            val base64 = if (skipPadding)
                Base64.getEncoder().withoutPadding().encode(bytes)
            else
                Base64.getEncoder().encode(bytes)

            // that base64 is what is eventually DB, and we have to encode again for JSON
            return Base64.getEncoder().encodeToString(base64)
        }

        override fun toSql(value: UUID, sb: SqlBuilder, topLevel: Boolean) {
            val bytes = value.toBytes()
            val base64 = if (skipPadding)
                Base64.getEncoder().withoutPadding().encode(bytes)
            else
                Base64.getEncoder().encode(bytes)

            sb.sql(toHexString(base64, "X'", "'"))
        }
    }

    private class BinaryHex(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {

        override fun fromJson(value: Any): UUID {
            val hexBytes = Base64.getDecoder().decode(value as String)
            val bytes = bytesFromHex(hexBytes)
            return bytes.toUUID()
        }

        override fun toJson(value: UUID): String {
            val hex = toHexBytes(value.toBytes())
            return Base64.getEncoder().encodeToString(hex)
        }

        override fun toSql(value: UUID, sb: SqlBuilder, topLevel: Boolean) {
            sb.sql("?").param(toJson(value))
        }
    }

    private class BinaryFullString(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {

        override fun fromJson(value: Any): UUID {
            val bytes = Base64.getDecoder().decode(value as String)
            return UUID.fromString(String(bytes, ISO_8859_1))
        }

        override fun toJson(value: UUID): String {
            val bytes = value.toString().toByteArray(ISO_8859_1)
            return Base64.getEncoder().encodeToString(bytes)
        }

        override fun toSql(value: UUID, sb: SqlBuilder, topLevel: Boolean) {
            val bytes = value.toString().toByteArray(ISO_8859_1)
            sb.sql(toHexString(bytes, "X'", "'"))
        }
    }

    companion object {

        fun create(concreteType: SqlTypeKind, typeSize: Int, isNotNull: Boolean): SqlTypeUUID {
            when (concreteType) {
                SqlTypeKind.CHAR, SqlTypeKind.VARCHAR ->
                    when (typeSize) {
                        16 -> return VarcharRawChars(isNotNull)
                        22 -> return VarcharBase64(isNotNull, skipPadding = true)
                        24 -> return VarcharBase64(isNotNull, skipPadding = false)
                        32 -> return VarcharHex(isNotNull)

                        else -> if (typeSize >= 36)
                            return VarcharFullString(isNotNull)
                    }

                SqlTypeKind.BINARY, SqlTypeKind.VARBINARY ->
                    when (typeSize) {
                        16 -> return BinaryRawChars(isNotNull)
                        22 -> return BinaryBase64(isNotNull, skipPadding = true)
                        24 -> return BinaryBase64(isNotNull, skipPadding = false)
                        32 -> return BinaryHex(isNotNull)

                        else -> if (typeSize >= 36)
                            return BinaryFullString(isNotNull)
                    }

            }

            throw IllegalArgumentException("Don't know how to convert $concreteType($typeSize) to UUID")
        }
    }
}

private fun ByteArray.toUUID(): UUID {
    val msb = extractLongBE(this, 0)
    val lsb = extractLongBE(this, 8)
    return UUID(msb, lsb)
}

private fun UUID.toBytes(): ByteArray {
    val bytes = ByteArray(16)
    putLongBE(mostSignificantBits, bytes, 0)
    putLongBE(leastSignificantBits, bytes, 8)
    return bytes
}