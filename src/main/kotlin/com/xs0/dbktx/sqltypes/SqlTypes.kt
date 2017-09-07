package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.sqltypes.SqlTypeKind.*
import java.math.BigDecimal
import java.time.*

import java.util.UUID
import kotlin.reflect.KClass

@PublishedApi
internal object SqlTypes {
    fun makeUUID(sqlType: SqlTypeKind, size: Int, isNotNull: Boolean): SqlType<UUID> {
        return SqlTypeUUID.create(sqlType, size, isNotNull)
    }

    fun makeString(sqlType: SqlTypeKind, size: Int?, isNotNull: Boolean): SqlType<String> {
        when (sqlType) {
            CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT ->
                return SqlTypeVarchar(sqlType, size, isNotNull)

            BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB ->
                return SqlTypeBinaryString(sqlType, size, isNotNull)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to String")
        }
    }

    fun makeInteger(sqlType: SqlTypeKind, isNotNull: Boolean, isAutoGenerated: Boolean, isUnsigned: Boolean): SqlType<Int> {
        when (sqlType) {
            TINYINT, SMALLINT, MEDIUMINT, INT ->
                return SqlTypeInt(sqlType, isNotNull, isAutoGenerated, isUnsigned)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to Integer")
        }
    }

    fun makeLong(sqlType: SqlTypeKind, isNotNull: Boolean, isAutoGenerated: Boolean, isUnsigned: Boolean): SqlType<Long> {
        when (sqlType) {
            INT, BIGINT ->
                return SqlTypeLong(sqlType, isNotNull, isAutoGenerated, isUnsigned)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to Long")
        }
    }

    fun makeFloat(sqlType: SqlTypeKind, isNotNull: Boolean, isUnsigned: Boolean): SqlType<Float> {
        when (sqlType) {
            FLOAT ->
                return SqlTypeFloat(sqlType, isNotNull = isNotNull, isUnsigned = isUnsigned)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to Float")
        }
    }

    fun makeDouble(sqlType: SqlTypeKind, isNotNull: Boolean, isUnsigned: Boolean): SqlType<Double> {
        when (sqlType) {
            FLOAT, DOUBLE ->
                return SqlTypeDouble(sqlType, isNotNull = isNotNull, isUnsigned = isUnsigned)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to Double")
        }
    }

    fun makeByteArray(sqlType: SqlTypeKind, size: Int?, isNotNull: Boolean): SqlType<ByteArray> {
        when (sqlType) {
            TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, BINARY, VARBINARY ->
                return SqlTypeBlob(sqlType, size ?: 0, isNotNull)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to byte[]")
        }
    }

    fun makeLocalDate(sqlType: SqlTypeKind, isNotNull: Boolean): SqlType<LocalDate> {
        when (sqlType) {
            DATE ->
                return SqlTypeLocalDate(sqlType, isNotNull = isNotNull)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to LocalDate")
        }
    }

    fun makeLocalDateTime(sqlType: SqlTypeKind, isNotNull: Boolean): SqlType<LocalDateTime> {
        when (sqlType) {
            DATETIME ->
                return SqlTypeLocalDateTime(sqlType, isNotNull = isNotNull)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to LocalDateTime")
        }
    }

    fun makeInstant(sqlType: SqlTypeKind, isNotNull: Boolean): SqlType<Instant> {
        when (sqlType) {
            TIMESTAMP ->
                return SqlTypeInstant(sqlType, isNotNull = isNotNull)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to Instant")
        }
    }

    fun makeYear(sqlType: SqlTypeKind, isNotNull: Boolean): SqlType<Year> {
        when (sqlType) {
            YEAR ->
                return SqlTypeYear(sqlType, isNotNull)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to Year")
        }
    }

    fun makeDuration(sqlType: SqlTypeKind, isNotNull: Boolean): SqlType<Duration> {
        when (sqlType) {
            TIME ->
                return SqlTypeDuration(sqlType, isNotNull = isNotNull)

            else ->
                throw UnsupportedOperationException("No mapping from $sqlType to Duration")
        }
    }

    fun makeLocalTime(sqlType: SqlTypeKind, isNotNull: Boolean): SqlType<LocalTime> {
        when (sqlType) {
            TIME -> return SqlTypeLocalTime(sqlType, isNotNull)

            else -> throw UnsupportedOperationException("No mapping from $sqlType to LocalTime")
        }
    }

//    fun makeBoolean(sqlType: SqlTypeKind, fieldProps: FieldProps): SqlType<Boolean> {
//        when (sqlType) {
//            TINYINT, SMALLINT, MEDIUMINT, INT ->
//               return SqlTypeIntBoolean(sqlType, fieldProps)
//
//            else ->
//                throw UnsupportedOperationException("No mapping from $sqlType to Boolean")
//        }
//    }

    fun makeEnumString(sqlType: SqlTypeKind, enums: Set<String>, isNotNull: Boolean): SqlType<String> {
        if (sqlType != ENUM)
            throw IllegalArgumentException("No mapping from $sqlType to String enum")

        return SqlTypeEnumString(enums, isNotNull)
    }

    fun makeBigDecimal(sqlType: SqlTypeKind, precision: Int, scale: Int, isNotNull: Boolean, isUnsigned: Boolean): SqlType<BigDecimal> {
        return SqlTypeBigDecimal(sqlType, precision, scale, isNotNull = isNotNull, isUnsigned = isUnsigned)
    }

    internal fun <ENUM : Enum<ENUM>> makeEnumToInt(klass: KClass<ENUM>, dummyValue: ENUM, sqlType: SqlTypeKind, toDbRep: (ENUM)->Int, fromDbRep: (Int)->ENUM, isNotNull: Boolean): SqlType<ENUM> {
        when (sqlType) {
            TINYINT, SMALLINT, MEDIUMINT, INT -> {
                return SqlTypeEnumInt(klass, toDbRep, fromDbRep, isNotNull, dummyValue)
            }

            else -> throw UnsupportedOperationException("No mapping from $sqlType to int(enum)")
        }
    }
}
