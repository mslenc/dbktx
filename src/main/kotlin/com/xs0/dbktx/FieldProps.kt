package com.xs0.dbktx

class FieldProps(
    val isPrimaryKey: Boolean,
    val foreignKey: ForeignKey<*,*>?,
    val isNotNull: Boolean,
    val isUnsigned: Boolean,
    val isAutoIncrement: Boolean,
    val falseValue: Int?,
    val trueValue: Int?,
    val collation: DbCollation?) {

    companion object {

        fun make(props: Array<FieldProp>, colType: String, vararg extraKindsSupported: FieldPropKind): FieldProps {
            return make(props, colType, null, *extraKindsSupported)
        }

        fun make(props: Array<FieldProp>, colType: String, defaultCollation: DbCollation?, vararg extraKindsSupported: FieldPropKind): FieldProps {
            var collation = defaultCollation
            val kinds = setOf(FieldPropKind.PRIMARY_KEY, FieldPropKind.FOREIGN_KEY, FieldPropKind.NOT_NULL, *extraKindsSupported)

            var foreignKey: ForeignKey<*,*>? = null
            var isPrimaryKey = false
            var notNull = false
            var unsigned = false
            var autoIncrement = false

            var boolValuesImplicit = true
            var falseValue: Int? = 0
            var trueValue: Int? = null

            var collationImplicit = true

            if (kinds.contains(FieldPropKind.COLLATE) && collation == null)
                throw IllegalArgumentException("Fields with collation must have default collation specified")

            for (prop in props) {
                if (!kinds.contains(prop.kind))
                    throw IllegalArgumentException("${prop.kind} not supported on $colType columns")

                when (prop.kind) {
                    FieldPropKind.PRIMARY_KEY -> isPrimaryKey = true

                    FieldPropKind.FOREIGN_KEY -> foreignKey = prop as ForeignKey<*,*>

                    FieldPropKind.NOT_NULL -> notNull = true

                    FieldPropKind.UNSIGNED -> unsigned = true

                    FieldPropKind.COLLATE -> {
                        if (collationImplicit) {
                            collationImplicit = false
                        } else {
                            throw IllegalArgumentException("Collation was already specified")
                        }
                        collation = (prop as Collate).collation
                    }

                    FieldPropKind.INT_AS_BOOL_VALUE_DEF -> {
                        if (boolValuesImplicit) {
                            boolValuesImplicit = false
                            trueValue = null
                            falseValue = trueValue
                        }

                        val def = prop as IntAsBoolValueDef
                        if (def.valueDefined) {
                            if (trueValue != null)
                                throw IllegalArgumentException("value for true is already defined")
                            trueValue = def.intValue
                        } else {
                            if (falseValue != null)
                                throw IllegalArgumentException("value for false is already defined")
                            falseValue = def.intValue
                        }
                    }

                    FieldPropKind.AUTO_GENERATED -> autoIncrement = true
                }
            }

            return FieldProps(isPrimaryKey, foreignKey, notNull, unsigned, autoIncrement, falseValue, trueValue, collation)
        }
    }
}
