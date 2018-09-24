package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.dbktx.util.*
import com.github.mslenc.dbktx.util.SpecialValues.emptyUUID
import java.util.UUID

import java.nio.charset.StandardCharsets.ISO_8859_1
import kotlin.reflect.KClass

abstract class SqlTypeUUID protected constructor(isNotNull: Boolean) : SqlType<UUID>(isNotNull = isNotNull) {
    override val dummyValue: UUID = UUID.randomUUID()

    override val kotlinType: KClass<UUID> = UUID::class

    override fun encodeForJson(value: UUID): Any {
        return if (value === emptyUUID) "" else value.toString()
    }

    override fun decodeFromJson(value: Any): UUID {
        if (value is CharSequence) {
            return if (value == "") {
                emptyUUID
            } else {
                UUID.fromString(value.toString())
            }
        }

        throw IllegalArgumentException("Expected a UUID String")
    }

    private class VarcharHex(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {
        override fun parseRowDataValue(value: Any): UUID {
            if (value is UUID)
                return value

            if (value is CharSequence) {
                return if (value.isNotEmpty()) {
                    bytesFromHex(value.toString()).toUUID()
                } else {
                    emptyUUID
                }
            }

            throw IllegalArgumentException("Expected a 32-character hex string")
        }

        override fun toSql(value: UUID, sql: Sql) {
            if (value === emptyUUID) {
                sql("")
            } else {
                sql(toHexString(value.toBytes()))
            }
        }
    }

    private class VarcharFullString(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {
        override fun parseRowDataValue(value: Any): UUID {
            if (value is UUID)
                return value

            if (value is CharSequence) {
                return if (value.isNotEmpty())
                    UUID.fromString(value.toString())
                else
                    emptyUUID
            }

            throw IllegalArgumentException("Expected a 36-character UUID string")
        }

        override fun toSql(value: UUID, sql: Sql) {
            if (value === emptyUUID) {
                sql.raw("''")
            } else {
                sql(value.toString())
            }
        }
    }

    private class BinaryRawChars(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {
        override fun parseRowDataValue(value: Any): UUID {
            if (value is UUID)
                return value

            if (value is ByteArray) {
                return if (value.size == 0) {
                    emptyUUID
                } else {
                    value.toUUID()
                }
            }

            throw IllegalArgumentException("Expected 16 bytes of UUID")
        }

        override fun toSql(value: UUID, sql: Sql) {
            if (value === emptyUUID) {
                sql.raw("''")
            } else {
                sql(value.toBytes())
            }
        }
    }

    private class BinaryFullString(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {
        override fun parseRowDataValue(value: Any): UUID {
            if (value is UUID)
                return value

            if (value is ByteArray) {
                if (value.size == 0)
                    return emptyUUID

                if (value.size == 36)
                    return UUID.fromString(String(value, ISO_8859_1))
            }

            throw IllegalArgumentException("Expected 36 bytes of UUID in string form")
        }

        override fun toSql(value: UUID, sql: Sql) {
            if (value === emptyUUID) {
                sql.raw("''")
            } else {
                val bytes = value.toString().toByteArray(ISO_8859_1)
                sql(bytes)
            }
        }
    }

    companion object {

        fun create(concreteType: SqlTypeKind, typeSize: Int, isNotNull: Boolean): SqlTypeUUID {
            when (concreteType) {
                SqlTypeKind.CHAR, SqlTypeKind.VARCHAR ->
                    when (typeSize) {
                        32 -> return VarcharHex(isNotNull)

                        else -> if (typeSize >= 36)
                            return VarcharFullString(isNotNull)
                    }

                SqlTypeKind.BINARY, SqlTypeKind.VARBINARY ->
                    when (typeSize) {
                        16 -> return BinaryRawChars(isNotNull)

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