package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueBigDecimal
import com.github.mslenc.dbktx.util.Sql
import java.math.BigDecimal
import kotlin.reflect.KClass

class SqlTypeBigDecimal(concreteType: SqlTypeKind,
                        val precision: Int,
                        val scale: Int,
                        isNotNull: Boolean,
                        isUnsigned: Boolean)
    : SqlTypeNumeric<BigDecimal>(isNotNull = isNotNull, isAutoGenerated = false, isUnsigned = isUnsigned) {

    init {
        if (concreteType != SqlTypeKind.DECIMAL)
            throw IllegalArgumentException("Unsupported type $concreteType")

        if (precision !in 1..65)
            throw IllegalArgumentException("Invalid precision")
        if (scale !in 0..minOf(30, precision))
            throw IllegalArgumentException("Invalid scale")
    }

    override fun parseDbValue(value: DbValue): BigDecimal {
        return value.asBigDecimal()
    }

    override fun makeDbValue(value: BigDecimal): DbValue {
        return DbValueBigDecimal(value)
    }

    override fun encodeForJson(value: BigDecimal): Any {
        return value.toPlainString()
    }

    override fun decodeFromJson(value: Any): BigDecimal {
        if (value is BigDecimal)
            return value

        if (value is CharSequence)
            return BigDecimal(value.toString())

        if (value is Number) {
            if (value is Double || value is Float)
                return BigDecimal(value.toDouble())

            return BigDecimal(value.toLong())
        }

        throw IllegalArgumentException("Not a recognized BigDecimal value - $value (${value.javaClass})")
    }

    override fun toSql(value: BigDecimal, sql: Sql) {
        sql(value)
    }

    override val dummyValue: BigDecimal = BigDecimal.ONE

    override val kotlinType: KClass<BigDecimal> = BigDecimal::class
}