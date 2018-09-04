package com.xs0.dbktx.schema

import com.xs0.asyncdb.common.RowData
import com.xs0.dbktx.composite.CompositeId
import com.xs0.dbktx.crud.BoundMultiColumnForSelect
import com.xs0.dbktx.expr.*
import com.xs0.dbktx.crud.EntityValues
import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.util.FakeRowData

class MultiColumnKeyDef<E : DbEntity<E, *>, ID : CompositeId<E, ID>>(
        override val table: DbTable<E, *>,
        override val indexInTable: Int,
        private val constructor: (RowData) -> ID,
        private val extractor: (E) -> ID,
        private val prototype: ID,
        override val isPrimaryKey: Boolean) : UniqueKeyDef<E, ID> {

    override val numColumns: Int
        get() = prototype.numColumns

    override fun makeLiteral(value: ID): Expr<E, ID> {
        return value
    }

    override fun getColumn(index: Int): NonNullColumn<E, *> {
        return prototype.getColumn(index)
    }

    override operator fun invoke(row: RowData): ID {
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