package com.xs0.dbktx

class ColumnMapping<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>, TYPE: Any>(
        val columnFrom: Column<FROM, TYPE>,
        val columnTo: Column<TO, TYPE>) {

    init {
        if (columnFrom.sqlType.kotlinType !== columnTo.sqlType.kotlinType)
            throw IllegalArgumentException("Mismatch between column types")
    }
}