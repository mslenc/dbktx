package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.FieldProps
import com.xs0.dbktx.SqlBuilder
import java.time.Duration
import kotlin.reflect.KClass

class SqlTypeDuration(concreteType: SqlTypeKind, fieldProps: FieldProps) : SqlType<Duration>(fieldProps) {
    init {
        if (concreteType != SqlTypeKind.TIME)
            throw IllegalArgumentException("Unsupported type " + concreteType)
    }

    override fun fromJson(value: Any): Duration {
        if (value !is CharSequence)
            throw IllegalArgumentException("Not a string(time/duration) value - " + value)

        val strValue = value.toString()

        try {
            val tmp = strValue.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            if (tmp.size == 3) {
                val hours = Duration.ofHours(Integer.parseInt(tmp[0]).toLong())
                val minutes = Duration.ofHours(Integer.parseInt(tmp[1]).toLong())
                val tmp2 = tmp[2].split("[.]".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                val seconds = Duration.ofSeconds(Integer.parseInt(tmp2[0]).toLong())
                if (tmp2.size > 1) {
                    var nanos = java.lang.Long.parseLong(tmp2[1])
                    var l = tmp2[1].length
                    if (l == 6) {
                        nanos *= 1000
                    } else {
                        while (l++ < 9)
                            nanos *= 10
                    }

                    return Duration.ofNanos(nanos).plus(seconds).plus(minutes).plus(hours)
                } else {
                    return seconds.plus(minutes).plus(hours)
                }
            } else {
                throw IllegalArgumentException("Unrecognized TIME value \"" + strValue + "\"")
            }
        } catch (e: Exception) {
            throw IllegalArgumentException("Unrecognized TIME value \"" + strValue + "\"")
        }

    }

    override fun toJson(value: Duration): Any {
        return formatDuration(value)
    }

    override fun dummyValue(): Duration {
        var duration = Duration.ofHours(3)
        duration = duration.plusMinutes(14)
        duration = duration.plusSeconds(15)
        duration = duration.plusNanos(926535897)
        return duration
    }

    override fun toSql(value: Duration, sb: SqlBuilder, topLevel: Boolean) {
        val formatted = formatDuration(value)
        sb.sql("?").param(formatted)
    }

    override val kotlinType: KClass<Duration> = Duration::class

    companion object {

        fun formatDuration(value: Duration): String {
            val micros = value.nano / 1000
            var seconds = value.seconds
            var minutes = seconds / 60
            seconds %= 60
            val hours = minutes / 60
            minutes %= 60

            val formatted: String
            if (micros > 0) {
                formatted = String.format("%02d:%02d:%02d.%06d", hours, minutes, seconds, micros)
            } else {
                formatted = String.format("%02d:%02d:%02d", hours, minutes, seconds)
            }
            return formatted
        }
    }
}