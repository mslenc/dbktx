package com.github.mslenc.dbktx.sqltypes

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.DbValueULong
import com.github.mslenc.asyncdb.util.ULong
import com.github.mslenc.dbktx.util.Sql
import kotlin.reflect.KClass

class SqlTypeULong(
        private val concreteType: SqlTypeKind,
        isNotNull: Boolean,
        isAutoGenerated: Boolean,
        isUnsigned: Boolean)

    : SqlTypeNumeric<ULong>(isNotNull = isNotNull,
                            isAutoGenerated = isAutoGenerated,
                            isUnsigned = isUnsigned) {

    init {
        if (!isUnsigned)
            throw IllegalArgumentException("Only use ULong for unsigned columns")

        when (concreteType) {
            SqlTypeKind.TINYINT,
            SqlTypeKind.SMALLINT,
            SqlTypeKind.MEDIUMINT,
            SqlTypeKind.INT,
            SqlTypeKind.BIGINT -> {
                // ok
            }

            else ->
                throw IllegalArgumentException("Unsupported type $concreteType")
        }
    }

    override fun parseDbValue(value: DbValue): ULong {
        return value.asULong()
    }

    override fun makeDbValue(value: ULong): DbValue {
        return DbValueULong(value)
    }

    override fun encodeForJson(value: ULong): Any {
        return value.toString()
    }

    override fun decodeFromJson(value: Any): ULong {
        return ULong.parseULong(value.toString())
    }

    override fun toSql(value: ULong, sql: Sql) {
        sql(value)
    }

    override val zeroValue: ULong = ULong.valueOf(0)

    override val kotlinType: KClass<ULong> = ULong::class
}