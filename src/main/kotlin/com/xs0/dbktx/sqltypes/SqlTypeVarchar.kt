package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.expr.Literal
import com.xs0.dbktx.util.SqlBuilder
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

            SqlTypeKind.LONGTEXT -> this.maxSize = Integer.MAX_VALUE

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

    override fun fromJson(value: Any): String {
        if (value is CharSequence)
            return value.toString()

        throw IllegalStateException("Not a string value: " + value.javaClass)
    }

    override fun toJson(value: String): Any {
        return value
    }

    override fun dummyValue(): String {
        var string = "properstring"
        if (string.length > maxSize)
            string = string.substring(0, maxSize)
        return string
    }

    override fun toSql(value: String, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value)
    }

    override val kotlinType: KClass<String> = String::class

    companion object {
        // is expected to only be used for calling the toSql(), so it is not expected to matter much which type it is, or which collation
        private val INSTANCE_FOR_LITERALS = SqlTypeVarchar(SqlTypeKind.LONGTEXT, null, true)

        fun <E> makeLiteral(value: String): Literal<E, String> {
            return Literal(value, INSTANCE_FOR_LITERALS)
        }
    }
}