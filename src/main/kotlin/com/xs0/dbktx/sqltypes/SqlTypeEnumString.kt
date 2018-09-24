package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.Sql
import kotlin.reflect.KClass
import kotlin.reflect.full.cast

class SqlTypeEnumString<ENUM : Enum<ENUM>>(override val kotlinType: KClass<ENUM>, private val toDbRep: (ENUM)->String, private val fromDbRep: (String)->ENUM, isNotNull: Boolean, override val dummyValue: ENUM) : SqlType<ENUM>(isNotNull = isNotNull) {
    override fun parseRowDataValue(value: Any): ENUM {
        if (value is CharSequence)
            return fromDbRep(value.toString())

        if (kotlinType.isInstance(value))
            return kotlinType.cast(value)

        throw IllegalStateException("Expected a string (CharSequence), but got ${value::class}")
    }

    override fun encodeForJson(value: ENUM): Any {
        return toDbRep(value)
    }

    override fun decodeFromJson(value: Any): ENUM {
        return parseRowDataValue(value)
    }

    override fun toSql(value: ENUM, sql: Sql) {
        sql(toDbRep(value))
    }
}
