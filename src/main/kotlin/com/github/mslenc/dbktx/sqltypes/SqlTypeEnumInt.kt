package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueInt
import com.github.mslenc.dbktx.util.Sql
import java.lang.IllegalArgumentException
import kotlin.reflect.KClass

@PublishedApi
internal class SqlTypeEnumInt<ENUM : Enum<ENUM>>(override val kotlinType: KClass<ENUM>, private val toDbRep: (ENUM)->Int, private val fromDbRep: (Int)->ENUM, isNotNull: Boolean, override val dummyValue: ENUM) : SqlType<ENUM>(isNotNull = isNotNull) {
    override fun parseDbValue(value: DbValue): ENUM {
        return fromDbRep(value.asInt())
    }

    override fun makeDbValue(value: ENUM): DbValue {
        return DbValueInt(toDbRep(value))
    }

    override fun encodeForJson(value: ENUM): Int {
        return toDbRep(value)
    }

    override fun decodeFromJson(value: Any): ENUM {
        if (value is Number)
            return fromDbRep(value.toInt())

        throw IllegalArgumentException("Not a number: $value")
    }

    override fun toSql(value: ENUM, sql: Sql) {
        sql(toDbRep(value))
    }
}
