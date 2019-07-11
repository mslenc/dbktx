package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.expr.FilterExpr

class RelToZeroOrOneImpl<FROM : DbEntity<FROM, FROM_KEY>, FROM_KEY: Any, TO : DbEntity<TO, *>> : RelToZeroOrOne<FROM, TO> {
    internal lateinit var info: ManyToOneInfo<TO, FROM, FROM_KEY>
    private lateinit var reverseKeyMapper: (TO)->FROM_KEY?
    private lateinit var queryExprBuilder: (Set<FROM_KEY>, TableInQuery<TO>)-> FilterExpr
    private lateinit var oppositeRel: RelToOneImpl<TO, FROM, FROM_KEY>
    private lateinit var oppositeColumn: Column<TO, FROM_KEY>

    internal fun init(oppositeRel: RelToOneImpl<TO, FROM, FROM_KEY>, info: ManyToOneInfo<TO, FROM, FROM_KEY>, reverseKeyMapper: (TO)->FROM_KEY?, queryExprBuilder: (Set<FROM_KEY>, TableInQuery<TO>)-> FilterExpr) {
        if (info.columnMappings.size != 1)
            throw UnsupportedOperationException("Multi-column relToZeroOrOne not supported yet")

        this.oppositeRel = oppositeRel
        this.info = info
        this.reverseKeyMapper = reverseKeyMapper
        this.queryExprBuilder = queryExprBuilder

        this.oppositeColumn = (info.columnMappings[0].columnFromAsNullable as? NullableColumn<TO, FROM_KEY>) ?: throw UnsupportedOperationException("With relToZeroOrOne, the opposite column must be nullable and of the same type as source primary key")
        if (info.oneTable.primaryKey.getColumn(1).sqlType.kotlinType != oppositeColumn.sqlType.kotlinType)
            throw UnsupportedOperationException("With relToZeroOrOne, the opposite column must be nullable and of the same type as source primary key")
    }

    override val targetTable: DbTable<TO, *>
        get() = info.manyTable

    override suspend fun invoke(from: FROM): TO? {
        return from.db.load(this, from)
    }

    override fun nullResult(): TO? {
        return null
    }

    override suspend fun loadNow(keys: Set<FROM>, db: DbConn): Map<FROM, TO?> {
        val index = keys.associateBy { it.id }
        val query = targetTable.newQuery(db)
        query.filter { oppositeColumn oneOf index.keys }

        val result = HashMap<FROM, TO?>()
        for (target in query.run()) {
            val id = oppositeColumn(target)
            val entity = index[id] ?: continue
            result[entity] = target
        }

        return result
    }
}