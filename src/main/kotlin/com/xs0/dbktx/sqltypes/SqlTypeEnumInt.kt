package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.Sql
import kotlin.reflect.KClass

@PublishedApi
internal class SqlTypeEnumInt<ENUM : Enum<ENUM>>(override val kotlinType: KClass<ENUM>, private val toDbRep: (ENUM)->Int, private val fromDbRep: (Int)->ENUM, isNotNull: Boolean, override val dummyValue: ENUM) : SqlType<ENUM>(isNotNull = isNotNull) {
    override fun parseRowDataValue(value: Any): ENUM {
        if (value is Number) {
            return fromDbRep(value.toInt())
        } else {
            throw IllegalStateException("Expected a number, but got ${value::class}")
        }
    }

    override fun encodeForJson(value: ENUM): Int {
        return toDbRep(value)
    }

    override fun decodeFromJson(value: Any): ENUM {
        return parseRowDataValue(value)
    }

    override fun toSql(value: ENUM, sql: Sql) {
        sql(toDbRep(value))
    }
}
