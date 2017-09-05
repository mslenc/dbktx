package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.FieldProps
import com.xs0.dbktx.SqlBuilder
import java.math.BigDecimal
import kotlin.reflect.KClass

class SqlTypeBigDecimal(concreteType: SqlTypeKind, private val precision: Int, private val scale: Int, fieldProps: FieldProps) : SqlTypeNumeric<BigDecimal>(fieldProps) {

    init {
        if (concreteType != SqlTypeKind.DECIMAL)
            throw IllegalArgumentException("Unsupported type " + concreteType)
    }

    override fun fromJson(value: Any): BigDecimal {
        if (value is CharSequence)
            return BigDecimal(value.toString())

        throw IllegalArgumentException("Not a string(bigdecimal) value - " + value)
    }

    override fun toJson(value: BigDecimal): String {
        return value.toString()
    }

    override fun dummyValue(): BigDecimal {
        return BigDecimal.ONE
    }

    override fun toSql(value: BigDecimal, sb: SqlBuilder, topLevel: Boolean) {
        sb.sql("?").param(value.toString())
    }

    override val kotlinType: KClass<BigDecimal> = BigDecimal::class
}
