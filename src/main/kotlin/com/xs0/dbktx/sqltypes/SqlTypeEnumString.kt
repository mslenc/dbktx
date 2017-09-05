package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.FieldProps
import com.xs0.dbktx.SqlBuilder
import kotlin.reflect.KClass

// enum => String
class SqlTypeEnumString(private val values: Map<String, Int>, fieldProps: FieldProps) : SqlType<String>(fieldProps) {
    private val collation = fieldProps.collation ?: throw IllegalArgumentException("Missing collation")

    override fun fromJson(value: Any): String {
        if (value is CharSequence)
            return value.toString()

        throw IllegalArgumentException("Not a string - " + value)
    }

    override fun toJson(value: String): String {
        return value
    }

    override fun dummyValue(): String {
        return values.keys.iterator().next()
    }

    override fun toSql(value: String, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value)
    }

    override val kotlinType: KClass<String> = String::class
}