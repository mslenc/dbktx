package com.xs0.dbktx

import com.xs0.dbktx.sqltypes.SqlTypeKind
import java.util.LinkedHashMap
import kotlin.reflect.KClass

abstract class FieldProp protected constructor() {

    abstract val kind: FieldPropKind

    companion object {
        object NOT_NULL : FieldProp() {
            override val kind = FieldPropKind.NOT_NULL
        }

        object UNSIGNED : FieldProp() {
            override val kind = FieldPropKind.UNSIGNED
        }

        object PRIMARY_KEY : FieldProp() {
            override val kind = FieldPropKind.PRIMARY_KEY
        }

        object AUTO_INCREMENT : FieldProp() {
            override val kind = FieldPropKind.AUTO_GENERATED
        }

        fun <E : DbEntity<E, ID>, ID> REFERENCES(foreignClass: KClass<E>): ForeignKey<E, ID> {
            return ForeignKey(foreignClass)
        }

        fun COLLATE(dbCollationName: String): Collate {
            // TODO: MySQL vs Postgres should be specified in DbSchema and resolved in DbTableBuilder
            return Collate(MySQLCollators.byName(dbCollationName))
        }

        fun ENUM(vararg values: String): EnumDef {
            if (values.isEmpty())
                throw IllegalArgumentException("Missing enum values")

            return EnumDef(*values)
        }

        fun TRUE_IS(value: Int): IntAsBoolValueDef {
            return IntAsBoolValueDef(true, value)
        }

        fun FALSE_IS(value: Int): IntAsBoolValueDef {
            return IntAsBoolValueDef(false, value)
        }

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
    }
}

class ForeignKey<E : DbEntity<E, ID>, ID> internal constructor(val foreignClass: KClass<E>) : FieldProp() {
    override val kind = FieldPropKind.FOREIGN_KEY
}

class EnumDef(vararg values: String) {
    private val values: LinkedHashMap<String, Int> = LinkedHashMap()

    init {
        for (value in values)
            if (this.values.put(value, this.values.size) != null)
                throw IllegalArgumentException("Repeated enum value")

        if (this.values.isEmpty())
            throw IllegalArgumentException("Missing enum values")
    }

    fun getValues(): Map<String, Int> {
        return values
    }
}

class SqlTypeDef internal constructor(
        val sqlTypeKind: SqlTypeKind,
        val param1: Int? = null,
        val param2: Int? = null
)

class Collate internal constructor(val collation: DbCollation) : FieldProp() {
    override val kind = FieldPropKind.COLLATE
}

class IntAsBoolValueDef(val valueDefined: Boolean, val intValue: Int) : FieldProp() {
    override val kind = FieldPropKind.INT_AS_BOOL_VALUE_DEF
}