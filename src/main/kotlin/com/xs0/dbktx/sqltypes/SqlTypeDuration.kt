package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.util.Sql
import java.time.Duration
import java.util.regex.Pattern
import kotlin.reflect.KClass

class SqlTypeDuration(concreteType: SqlTypeKind, isNotNull: Boolean) : SqlType<Duration>(isNotNull = isNotNull) {
    init {
        if (concreteType != SqlTypeKind.TIME)
            throw IllegalArgumentException("Unsupported type " + concreteType)
    }

    private val colonRegex = Pattern.compile(":")
    private val dotRegex = Pattern.compile("[.]")

    private fun fail(str: String): Nothing {
        throw IllegalArgumentException("Unrecognized TIME value \"$str\"")
    }

    override fun fromJson(value: Any): Duration {
        if (value !is CharSequence)
            throw IllegalArgumentException("Not a string(time/duration) value - " + value)

        val str = value.toString()

        val tmp = str.split(colonRegex)
        if (tmp.size != 3)
            fail(str)

        val hours = tmp[0].toIntOrNull() ?: fail(str)
        val minutes = tmp[1].toIntOrNull() ?: fail(str)

        val tmp2 = tmp[2].split(dotRegex)
        val seconds: Int
        val nanos: Int

        when (tmp2.size) {
            1 -> {
                seconds = tmp2[0].toIntOrNull() ?: fail(str)
                nanos = 0
            }
            2 -> {
                seconds = tmp2[0].toIntOrNull() ?: fail(str)
                nanos = tmp2[1].padEnd(9, '0').toIntOrNull() ?: fail(str)
            }
            else -> fail(str)
        }

        // TODO: observe postgres limits
        if (hours in -838..838 && minutes in 0..59 && seconds in 0..59 && nanos in 0..999999999)
            return Duration.ofNanos(hours * (60 * 60 * NS) +
                                    minutes * (60 * NS) +
                                    seconds * NS +
                                    nanos)

        fail(str)
    }

    override fun toJson(value: Duration): String {
        return Sql.formatDuration(value)
    }

    override fun toSql(value: Duration, sql: Sql) {
        sql(value)
    }

    override val dummyValue: Duration = Duration.ofSeconds(31415)

    override val kotlinType: KClass<Duration> = Duration::class

    companion object {
        const val NS: Long = 1_000_000_000
    }
}