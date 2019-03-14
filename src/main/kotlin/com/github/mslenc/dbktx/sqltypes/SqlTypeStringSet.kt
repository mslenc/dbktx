package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueString
import com.github.mslenc.dbktx.util.Sql
import com.github.mslenc.dbktx.util.StringSet
import com.github.mslenc.dbktx.util.toStringSet
import java.lang.IllegalArgumentException
import kotlin.reflect.KClass

class SqlTypeStringSet(private val concreteType: SqlTypeKind,
                       private val surroundedWithCommas: Boolean,
                       isNotNull: Boolean)
    : SqlType<StringSet>(isNotNull = isNotNull, isAutoGenerated = false) {

    init {
        // TODO: check concreteType is varchar or an actual SET, maximum length, etc..
    }

    override fun parseDbValue(value: DbValue): StringSet {
        return processString(value.asString())
    }

    override fun makeDbValue(value: StringSet): DbValue {
        return DbValueString(encodeForJson(value))
    }

    private fun processString(str: String): StringSet {
        val finalStr =
            if (surroundedWithCommas && str.startsWith(",") && str.endsWith(",")) {
                str.substring(1, str.length - 1)
            } else {
                str
            }

        if (finalStr.isEmpty())
            return StringSet()

        return finalStr.split(",").toSet().toStringSet()
    }

    override fun encodeForJson(value: StringSet): String {
        val sb = StringBuilder()

        if (surroundedWithCommas)
            sb.append(',')

        var first = true

        value.forEach {
            if (first) {
                first = false
            } else {
                sb.append(',')
            }

            sb.append(it)
        }

        if (surroundedWithCommas)
            sb.append(',')

        return sb.toString()
    }

    override fun decodeFromJson(value: Any): StringSet {
        if (value is String)
            return processString(value)

        throw IllegalArgumentException("Not a string: $value")
    }

    override val zeroValue: StringSet
        get() = emptySet<String>().toStringSet()

    override val kotlinType: KClass<StringSet>
        get() = StringSet::class

    override fun toSql(value: StringSet, sql: Sql) {
        sql(encodeForJson(value))
    }
}
