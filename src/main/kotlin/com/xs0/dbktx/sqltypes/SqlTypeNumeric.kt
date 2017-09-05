package com.xs0.dbktx.sqltypes

import com.xs0.dbktx.FieldProps

abstract class SqlTypeNumeric<T : Any> protected constructor(props: FieldProps) : SqlType<T>(props) {
    val isUnsigned: Boolean = props.isUnsigned
}
