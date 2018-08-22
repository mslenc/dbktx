package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.Sql
import kotlin.reflect.KClass

// enum => String
class SqlTypeEnumString<ENUM : Enum<ENUM>>(override val kotlinType: KClass<ENUM>, private val toDbRep: (ENUM)->String, private val fromDbRep: (String)->ENUM, isNotNull: Boolean, override val dummyValue: ENUM) : SqlType<ENUM>(isNotNull = isNotNull) {
    override fun fromJson(value: Any): ENUM {
        if (value is CharSequence) {
            return fromDbRep(value.toString())
        } else {
            throw IllegalStateException("Expected a string (CharSequence), but got ${value::class}")
        }
    }

    override fun toJson(value: ENUM): String {
        return toDbRep(value)
    }

    override fun toSql(value: ENUM, sql: Sql) {
        sql(toDbRep(value))
    }
}
