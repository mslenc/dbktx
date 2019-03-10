package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueLong
import com.github.mslenc.dbktx.util.Sql
import java.lang.IllegalArgumentException
import kotlin.reflect.KClass

@PublishedApi
internal class SqlTypeEnumLong<ENUM : Enum<ENUM>>(override val kotlinType: KClass<ENUM>, private val toDbRep: (ENUM)->Long, private val fromDbRep: (Long)->ENUM, isNotNull: Boolean, override val dummyValue: ENUM) : SqlType<ENUM>(isNotNull = isNotNull) {
    override fun parseDbValue(value: DbValue): ENUM {
        return fromDbRep(value.asLong())
    }

    override fun makeDbValue(value: ENUM): DbValue {
        return DbValueLong(toDbRep(value))
    }

    override fun encodeForJson(value: ENUM): Long {
        return toDbRep(value)
    }

    override fun decodeFromJson(value: Any): ENUM {
        if (value is Number)
            return fromDbRep(value.toLong())

        throw IllegalArgumentException("Not a number: $value")
    }

    override fun toSql(value: ENUM, sql: Sql) {
        sql(toDbRep(value))
    }
}
