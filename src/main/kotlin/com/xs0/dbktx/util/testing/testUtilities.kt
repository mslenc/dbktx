package com.xs0.dbktx.util.testing

import com.xs0.dbktx.schema.Column
import com.xs0.dbktx.schema.DbEntity
import io.vertx.core.json.JsonObject

fun <E: DbEntity<E, *>, T: Any>
JsonObject.addSqlValue(column: Column<E, T>, value: T?) {
    if (value == null) {
        this.putNull(column.fieldName)
    } else {
        this.put(column.fieldName, column.sqlType.toJson(value))
    }
}