package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.Sql
import kotlin.reflect.KClass

// enum => String
class SqlTypeEnumString(val values: Set<String>, isNotNull: Boolean) : SqlType<String>(isNotNull = isNotNull) {
    override val dummyValue: String

    init {
        if (values.isEmpty())
            throw IllegalArgumentException("Missing enum values")

        dummyValue = values.iterator().next()
    }

    override fun fromJson(value: Any): String {
        if (value is CharSequence)
            return value.toString()

        throw IllegalArgumentException("Not a string - " + value)
    }

    override fun toJson(value: String): String {
        return value
    }

    override fun toSql(value: String, sql: Sql) {
        sql(value)
    }

    override val kotlinType: KClass<String> = String::class
}