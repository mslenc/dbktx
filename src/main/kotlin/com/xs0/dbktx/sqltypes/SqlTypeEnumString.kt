package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.SqlBuilder
import kotlin.reflect.KClass

// enum => String
class SqlTypeEnumString(val values: Set<String>, isNotNull: Boolean) : SqlType<String>(isNotNull = isNotNull) {
    init {
        if (values.isEmpty())
            throw IllegalArgumentException("Missing enum values")
    }

    override fun fromJson(value: Any): String {
        if (value is CharSequence)
            return value.toString()

        throw IllegalArgumentException("Not a string - " + value)
    }

    override fun toJson(value: String): String {
        return value
    }

    override fun dummyValue(): String {
        return values.iterator().next()
    }

    override fun toSql(value: String, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value)
    }

    override val kotlinType: KClass<String> = String::class
}