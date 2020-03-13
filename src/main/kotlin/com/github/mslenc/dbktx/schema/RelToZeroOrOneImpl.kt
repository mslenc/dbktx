package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.expr.Expr

class RelToZeroOrOneImpl<FROM : DbEntity<FROM, FROM_KEY>, FROM_KEY: Any, TO : DbEntity<TO, *>> : RelToZeroOrOne<FROM, TO> {
    internal lateinit var info: ManyToOneInfo<TO, FROM, FROM_KEY>
    private lateinit var reverseKeyMapper: (TO)->FROM_KEY?
    private lateinit var queryExprBuilder: (Set<FROM_KEY>, TableInQuery<TO>)->Expr<Boolean>
    private lateinit var oppositeRel: RelToOneImpl<TO, FROM, FROM_KEY>
    private lateinit var oppositeColumn: Column<TO, FROM_KEY>
    private lateinit var relDebugName: String

    internal fun init(oppositeRel: RelToOneImpl<TO, FROM, FROM_KEY>, info: ManyToOneInfo<TO, FROM, FROM_KEY>, reverseKeyMapper: (TO)->FROM_KEY?, queryExprBuilder: (Set<FROM_KEY>, TableInQuery<TO>)->Expr<Boolean>) {
        if (info.columnMappings.size != 1)
            throw UnsupportedOperationException("Multi-column relToZeroOrOne not supported yet")

        this.oppositeRel = oppositeRel
        this.info = info
        this.reverseKeyMapper = reverseKeyMapper
        this.queryExprBuilder = queryExprBuilder

        this.oppositeColumn = (info.columnMappings[0].rawColumnFrom as Column<TO, FROM_KEY>)
        if (info.oneTable.primaryKey.getColumn(1).sqlType.kotlinType != oppositeColumn.sqlType.kotlinType)
            throw UnsupportedOperationException("With relToZeroOrOne, the opposite column must be of the same type as source primary key")

        this.relDebugName = "ToZeroOrOne(${ info.oneTable.dbName }->${ targetTable.dbName })"
    }

    override fun toString(): String {
        return relDebugName
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
        val query = targetTable.newEntityQuery(db)
        val opposite = this.oppositeColumn
        if (opposite is NonNullColumn) {
            query.filter { opposite oneOf index.keys }
        } else {
            query.filter { (opposite as NullableColumn) oneOf index.keys }
        }

        val result = HashMap<FROM, TO?>()
        for (target in query.execute()) {
            val id = oppositeColumn(target)
            val entity = index[id] ?: continue
            result[entity] = target
        }

        return result
    }
}