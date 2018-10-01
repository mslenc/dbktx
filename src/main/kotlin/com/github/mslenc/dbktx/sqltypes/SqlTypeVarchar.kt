package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.dbktx.expr.Literal
import com.github.mslenc.dbktx.util.Sql
import kotlin.reflect.KClass

// VARCHAR, TEXT, etc => String
class SqlTypeVarchar(
        concreteType: SqlTypeKind,
        size: Int?,
        isNotNull: Boolean)
    : SqlType<String>(isNotNull, false) {

    private val maxSize: Int

    init {
        when (concreteType) {
            SqlTypeKind.TINYTEXT -> this.maxSize = 255

            SqlTypeKind.TEXT -> this.maxSize = 65535

            SqlTypeKind.MEDIUMTEXT -> this.maxSize = 16777215

            SqlTypeKind.LONGTEXT,
            SqlTypeKind.JSON -> this.maxSize = Integer.MAX_VALUE

            SqlTypeKind.CHAR -> {
                if (size == null)
                    throw IllegalArgumentException("Null size")
                if (size > 255)
                    throw IllegalArgumentException("Invalid size, can be at most 255")
                this.maxSize = size
            }

            SqlTypeKind.VARCHAR -> {
                if (size == null)
                    throw IllegalArgumentException("Null size")
                if (size > 65535)
                    throw IllegalArgumentException("Invalid size, can be at most 65535")
                this.maxSize = size
            }

            else -> throw IllegalArgumentException("Unsupported type " + concreteType)
        }

        if (maxSize < 1)
            throw IllegalArgumentException("Invalid size, must be at least 1")
    }

    override fun parseRowDataValue(value: Any): String {
        if (value is CharSequence)
            return value.toString()

        throw IllegalStateException("Not a string value: class " + value.javaClass)
    }

    override fun encodeForJson(value: String): Any {
        return value
    }

    override fun decodeFromJson(value: Any): String {
        return parseRowDataValue(value)
    }

    override fun toSql(value: String, sql: Sql) {
        sql(value)
    }

    override val dummyValue: String = "properString".take(maxSize)

    override val kotlinType: KClass<String> = String::class

    companion object {
        // is expected to only be used for calling the toSql(), so it is not expected to matter much which type it is, or which collation
        private val INSTANCE_FOR_LITERALS = SqlTypeVarchar(SqlTypeKind.LONGTEXT, null, true)

        fun <E> makeLiteral(value: String): Literal<E, String> {
            return Literal(value, INSTANCE_FOR_LITERALS)
        }
    }
}