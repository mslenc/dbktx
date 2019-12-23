package com.github.mslenc.dbktx.schema

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.composite.CompositeId
import com.github.mslenc.dbktx.crud.BoundMultiColumnForSelect
import com.github.mslenc.dbktx.expr.*
import com.github.mslenc.dbktx.crud.EntityValues
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.util.FakeRowData

class MultiColumnKeyDef<E : DbEntity<E, *>, ID : CompositeId<E, ID>>(
        override val table: DbTable<E, *>,
        override val indexInTable: Int,
        private val constructor: (DbRow) -> ID,
        private val extractor: (E) -> ID,
        private val prototype: ID,
        override val isPrimaryKey: Boolean) : UniqueKeyDef<E, ID> {

    override val numColumns: Int
        get() = prototype.numColumns

    override fun makeLiteral(value: ID): Expr<ID> {
        return value
    }

    override fun getColumn(index: Int): NonNullColumn<E, *> {
        return prototype.getColumn(index)
    }

    override operator fun invoke(row: DbRow): ID {
        return constructor(row)
    }

    override fun invoke(entity: E): ID {
        return extractor(entity)
    }

    override val isAutoGenerated: Boolean
        get() = false

    override fun bindForSelect(tableInQuery: TableInQuery<E>): BoundMultiColumnForSelect<E, ID> {
        return BoundMultiColumnForSelect(this, tableInQuery)
    }

    override fun extract(values: EntityValues<E>): ID? {
        val row = FakeRowData()

        for (i in 1..numColumns) {
            val part = getColumn(i)
            if (!extractSingleColumnValue(part, values, row)) {
                return null
            }
        }

        return constructor(row)
    }

    private fun <T: Any> extractSingleColumnValue(part: NonNullColumn<E, T>, values: EntityValues<E>, row: FakeRowData): Boolean {
        val value = part.extract(values) ?: return false
        row.put(part, value)
        return true
    }
}