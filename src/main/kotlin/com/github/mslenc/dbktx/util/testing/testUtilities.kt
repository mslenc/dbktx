package com.github.mslenc.dbktx.util.testing

import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.schema.DbEntity
import io.vertx.core.json.JsonObject
import java.time.LocalDateTime

fun <E: DbEntity<E, *>, T: Any>
JsonObject.addSqlValue(column: Column<E, T>, value: T?) {
    if (value == null) {
        this.putNull(column.fieldName)
    } else {
        this.put(column.fieldName, column.sqlType.encodeForJson(value))
    }
}

fun CharSequence.toLDT(): LocalDateTime {
    return LocalDateTime.parse(this)
}