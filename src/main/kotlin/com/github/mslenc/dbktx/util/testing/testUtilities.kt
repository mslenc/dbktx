package com.github.mslenc.dbktx.util.testing

import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.asyncdb.impl.values.*
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

fun Any?.toDbValue(): DbValue {
    if (this == null)
        return DbValueNull.instance()

    if (this is String)
        return DbValueString(this)
    if (this is Int)
        return DbValueInt(this)
    if (this is LocalDateTime)
        return DbValueLocalDateTime(this)
    if (this is Double)
        return DbValueDouble(this)

    TODO(this.javaClass.toString())
}