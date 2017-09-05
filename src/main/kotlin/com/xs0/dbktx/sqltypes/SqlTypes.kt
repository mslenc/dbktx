package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.EnumDef
import com.xs0.dbktx.FieldProps
import com.xs0.dbktx.sqltypes.SqlTypeKind.*
import java.math.BigDecimal
import java.time.*

import java.util.UUID
import kotlin.reflect.KClass

internal object SqlTypes {
    fun makeUUID(sqlType: SqlTypeKind, size: Int, fieldProps: FieldProps): SqlType<UUID> {
        return SqlTypeUUID.create(sqlType, size, fieldProps)
    }

    fun makeString(sqlType: SqlTypeKind, size: Int?, fieldProps: FieldProps): SqlType<String> {
        when (sqlType) {
            CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT ->
                return SqlTypeVarchar(sqlType, size, fieldProps)

            BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB ->
                return SqlTypeBinaryString(sqlType, size, fieldProps)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to String")
        }
    }

    fun makeInteger(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<Int> {
        when (sqlType) {
            TINYINT, SMALLINT, MEDIUMINT, INT -> return SqlTypeInt(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to Integer")
        }
    }

    fun makeLong(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<Long> {
        when (sqlType) {
            INT, BIGINT -> return SqlTypeLong(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to Long")
        }
    }

    fun makeFloat(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<Float> {
        when (sqlType) {
            FLOAT -> return SqlTypeFloat(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to Float")
        }
    }

    fun makeDouble(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<Double> {
        when (sqlType) {
            FLOAT, DOUBLE -> return SqlTypeDouble(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to Double")
        }
    }

    fun makeByteArray(sqlType: SqlTypeKind, size: Int?, fieldProps: FieldProps): SqlType<ByteArray> {
        when (sqlType) {
            TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, BINARY, VARBINARY -> return SqlTypeBlob(sqlType, size ?: 0, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to byte[]")
        }
    }

    fun makeLocalDate(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<LocalDate> {
        when (sqlType) {
            DATE -> return SqlTypeLocalDate(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to LocalDate")
        }
    }

    fun makeLocalDateTime(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<LocalDateTime> {
        when (sqlType) {
            DATETIME -> return SqlTypeLocalDateTime(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to LocalDateTime")
        }
    }

    fun makeInstant(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<Instant> {
        when (sqlType) {
            TIMESTAMP -> return SqlTypeInstant(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to Instant")
        }
    }

    fun makeYear(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<Year> {
        when (sqlType) {
            YEAR -> return SqlTypeYear(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to Year")
        }
    }

    fun makeDuration(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<Duration> {
        when (sqlType) {
            TIME -> return SqlTypeDuration(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to Duration")
        }
    }

    fun makeLocalTime(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<LocalTime> {
        when (sqlType) {
            TIME -> return SqlTypeLocalTime(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to LocalTime")
        }
    }

    fun makeBoolean(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<Boolean> {
        when (sqlType) {
            TINYINT, SMALLINT, MEDIUMINT, INT -> return SqlTypeIntBoolean(sqlType, fieldProps)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to Boolean")
        }
    }

    fun makeEnumString(enumDef: EnumDef, fieldProps: FieldProps): SqlType<String> {
        return SqlTypeEnumString(enumDef.getValues(), fieldProps)
    }

    fun makeBigDecimal(sqlType: SqlTypeKind, precision: Int, scale: Int, fieldProps: FieldProps): SqlType<BigDecimal> {
        return SqlTypeBigDecimal(sqlType, precision, scale, fieldProps)
    }

    inline fun <reified ENUM : Enum<ENUM>> makeEnumToInt(klass: KClass<ENUM>, sqlType: SqlTypeKind, noinline toDbRep: (ENUM)->Int, noinline fromDbRep: (Int)->ENUM, fieldProps: FieldProps): SqlType<ENUM> {
        when (sqlType) {
            TINYINT, SMALLINT, MEDIUMINT, INT -> {
                val dummyValue: ENUM = enumValues<ENUM>()[0]
                return SqlTypeEnumInt(klass, toDbRep, fromDbRep, fieldProps, dummyValue)
            }

            else -> throw UnsupportedOperationException("No mapping from $sqlType to int(enum)")
        }
    }
}
