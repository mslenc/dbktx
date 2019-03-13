package com.github.mslenc.dbktx.util.testing

import com.github.mslenc.asyncdb.DbColumn
import com.github.mslenc.asyncdb.DbQueryResultObserver
import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.asyncdb.impl.DbColumnsImpl
import com.github.mslenc.asyncdb.impl.DbResultSetImpl
import com.github.mslenc.asyncdb.impl.DbRowImpl
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException

class MockDbColumn(val _name: String, val _indexInRow: Int) : DbColumn {
    override fun getIndexInRow(): Int {
        return _indexInRow
    }

    override fun getName(): String {
        return _name
    }
}

object MockResultSet {
    class Builder {
        private val columns = ArrayList<DbColumn>()
        private var dbColumns = DbColumnsImpl(columns)
        private val rows = ArrayList<DbRow>()

        constructor()

        constructor(vararg columnNames: String) {
            addColumns(*columnNames)
        }

        fun addColumns(vararg columnNames: String): Builder {
            if (rows.isNotEmpty())
                throw IllegalStateException("Can't add columns after already adding rows")

            for (name in columnNames)
                columns.add(MockDbColumn(name, columns.size))

            this.dbColumns = DbColumnsImpl(columns)
            return this
        }

        fun addRow(vararg values: Any?): Builder {
            if (values.size != columns.size)
                throw IllegalArgumentException("Mismatch in number of columns")

            rows.add(DbRowImpl.copyFrom(values.map(Any?::toDbValue), dbColumns, rows.size))

            return this
        }

        fun build(): DbResultSet {
            return DbResultSetImpl(dbColumns, rows)
        }

        fun streamInto(observer: DbQueryResultObserver) {
            for (row in rows)
                observer.onNext(row)
            observer.onCompleted()
        }
    }
}