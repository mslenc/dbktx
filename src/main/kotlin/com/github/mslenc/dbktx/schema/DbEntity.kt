package com.github.mslenc.dbktx.schema

abstract class DbEntity<E : DbEntity<E, ID>, ID: Any>(
    val id: ID,
) {

    abstract val metainfo: DbTable<E, ID>

    override fun toString(): String {
        val sb = StringBuilder()
        sb.append(metainfo.dbName).append('(')
        var first = true
        for (column in metainfo.columns) {
            if (first) {
                first = false
            } else {
                sb.append(", ")
            }

            val value = column.invoke(this as E).toString()

            val shortValue = if (value.length <= 40) {
                value
            } else {
                value.substring(0..17) + "[...]" + value.substring(value.length - 18)
            }

            sb.append(column.fieldName).append('=').append(shortValue)
        }
        sb.append(')')
        return sb.toString()
    }
}
