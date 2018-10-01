package com.github.mslenc.dbktx.fieldprops

import com.github.mslenc.dbktx.sqltypes.SqlTypeKind

fun CHAR(size: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.CHAR, size)
}

fun VARCHAR(size: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.VARCHAR, size)
}

fun TINYTEXT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.TINYTEXT)
}

fun TEXT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.TEXT)
}

fun MEDIUMTEXT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.MEDIUMTEXT)
}

fun LONGTEXT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.LONGTEXT)
}

fun BINARY(size: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.BINARY, size)
}

fun VARBINARY(size: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.VARBINARY, size)
}

fun TINYBLOB(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.TINYBLOB)
}

fun BLOB(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.BLOB)
}

fun MEDIUMBLOB(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.MEDIUMBLOB)
}

fun LONGBLOB(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.LONGBLOB)
}

fun TINYINT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.TINYINT)
}

fun TINYINT(size: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.TINYINT, size)
}

fun SMALLINT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.SMALLINT)
}

fun SMALLINT(size: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.SMALLINT, size)
}

fun MEDIUMINT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.MEDIUMINT)
}

fun INT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.INT)
}

fun INT(size: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.INT, size)
}

fun BIGINT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.BIGINT)
}

fun BIGINT(size: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.BIGINT, size)
}

fun DECIMAL(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.DECIMAL, 10, 0)
}

fun DECIMAL(precision: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.DECIMAL, precision, 0)
}

fun DECIMAL(precision: Int, scale: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.DECIMAL, precision, scale)
}

fun FLOAT(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.FLOAT)
}

fun DOUBLE(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.DOUBLE)
}

fun DATE(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.DATE)
}

fun DATETIME(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.DATETIME)
}

fun TIMESTAMP(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.TIMESTAMP)
}

fun TIME(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.TIME)
}

fun YEAR(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.YEAR)
}

fun YEAR(size: Int): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.YEAR, size)
}

fun ENUM(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.ENUM)
}

fun ENUM(vararg posibs: String): SqlTypeDef {
    // TODO: use posibs somehow?
    return SqlTypeDef(SqlTypeKind.ENUM)
}

fun JSON(): SqlTypeDef {
    return SqlTypeDef(SqlTypeKind.JSON)
}

class SqlTypeDef internal constructor(
    val sqlTypeKind: SqlTypeKind,
    val param1: Int? = null,
    val param2: Int? = null
)
