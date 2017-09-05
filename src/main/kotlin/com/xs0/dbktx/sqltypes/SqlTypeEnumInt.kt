package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.FieldProps
import com.xs0.dbktx.SqlBuilder
import kotlin.reflect.KClass

internal class SqlTypeEnumInt<ENUM : Enum<ENUM>>(override val kotlinType: KClass<ENUM>, private val toDbRep: (ENUM)->Int, private val fromDbRep: (Int)->ENUM, fieldProps: FieldProps, private val dummyValue: ENUM) : SqlType<ENUM>(fieldProps) {
    override fun fromJson(value: Any): ENUM {
        if (value is Number) {
            return fromDbRep(value.toInt())
        } else {
            throw IllegalStateException("Expected a number, but got ${value::class}")
        }
    }

    override fun toJson(value: ENUM): Any {
        return toDbRep(value)
    }

    override fun dummyValue(): ENUM {
        return dummyValue
    }

    override fun toSql(value: ENUM, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(toDbRep(value))
    }
}
