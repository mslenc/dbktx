package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueByteArray
import com.github.mslenc.asyncdb.impl.values.DbValueString
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
        override fun parseDbValue(value: DbValue): UUID {
            val str = value.asString()

            return if (str.isNotEmpty()) {
                bytesFromHex(str).toUUID()
            } else {
                emptyUUID
            }
        }

        override fun makeDbValue(value: UUID): DbValue {
            if (value === emptyUUID) {
                return DbValueString("")
            } else {
                return DbValueString(toHexString(value.toBytes()))
            }
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
        override fun parseDbValue(value: DbValue): UUID {
            val str = value.asString()

            return if (str.isNotEmpty()) {
                UUID.fromString(str)
            } else {
                emptyUUID
            }
        }

        override fun makeDbValue(value: UUID): DbValue {
            if (value === emptyUUID) {
                return DbValueString("")
            } else {
                return DbValueString(value.toString())
            }
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
        override fun parseDbValue(value: DbValue): UUID {
            val bytes = value.asByteArray()

            return if (bytes.isEmpty()) {
                emptyUUID
            } else {
                bytes.toUUID()
            }
        }

        override fun makeDbValue(value: UUID): DbValue {
            if (value === emptyUUID) {
                return DbValueByteArray(byteArrayOf())
            } else {
                return DbValueByteArray(value.toBytes())
            }
        }

        override fun toSql(value: UUID, sql: Sql) {
            if (value === emptyUUID) {
                sql.raw("''")
            } else {
                sql.invoke(value.toBytes())
            }
        }
    }

    private class BinaryFullString(isNotNull: Boolean) : SqlTypeUUID(isNotNull) {
        override fun parseDbValue(value: DbValue): UUID {
            val bytes = value.asByteArray()

            if (bytes.isEmpty())
                return emptyUUID

            if (bytes.size == 36)
                return UUID.fromString(bytes.toString(ISO_8859_1))

            throw IllegalArgumentException("Expected 36 bytes of UUID in string form")
        }

        override fun makeDbValue(value: UUID): DbValue {
            if (value === emptyUUID) {
                return DbValueByteArray(byteArrayOf())
            } else {
                return DbValueByteArray(value.toString().toByteArray(ISO_8859_1))
            }
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