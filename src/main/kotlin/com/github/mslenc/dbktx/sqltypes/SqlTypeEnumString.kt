package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueString
import com.github.mslenc.dbktx.util.Sql
import java.lang.IllegalArgumentException
import kotlin.reflect.KClass

class SqlTypeEnumString<ENUM : Enum<ENUM>>(override val kotlinType: KClass<ENUM>, private val toDbRep: (ENUM)->String, private val fromDbRep: (String)->ENUM, isNotNull: Boolean, override val zeroValue: ENUM) : SqlType<ENUM>(isNotNull = isNotNull) {
    override fun parseDbValue(value: DbValue): ENUM {
        return fromDbRep(value.asString())
    }

    override fun makeDbValue(value: ENUM): DbValue {
        return DbValueString(toDbRep(value))
    }

    override fun encodeForJson(value: ENUM): Any {
        return toDbRep(value)
    }

    override fun decodeFromJson(value: Any): ENUM {
        if (value is String)
            return fromDbRep(value)

        throw IllegalArgumentException("Not a string: $value")
    }

    override fun toSql(value: ENUM, sql: Sql) {
        sql(toDbRep(value))
    }
}
