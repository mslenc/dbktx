package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.crud.EntityQuery
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.expr.ScalarExprBuilder
import java.util.ArrayList

class RelToManyImpl<FROM : DbEntity<FROM, FROM_KEY>, FROM_KEY: Any, TO : DbEntity<TO, *>> : RelToMany<FROM, TO> {

    internal lateinit var info: ManyToOneInfo<TO, FROM, FROM_KEY>
    private lateinit var reverseKeyMapper: (TO)->FROM_KEY?
    private lateinit var queryExprBuilder: (Set<FROM_KEY>, TableInQuery<TO>)-> FilterExpr
    private lateinit var oppositeRel: RelToOneImpl<TO, FROM, FROM_KEY>

    internal fun init(oppositeRel: RelToOneImpl<TO, FROM, FROM_KEY>, info: ManyToOneInfo<TO, FROM, FROM_KEY>, reverseKeyMapper: (TO)->FROM_KEY?, queryExprBuilder: (Set<FROM_KEY>, TableInQuery<TO>)-> FilterExpr) {
        this.oppositeRel = oppositeRel
        this.info = info
        this.reverseKeyMapper = reverseKeyMapper
        this.queryExprBuilder = queryExprBuilder
    }

    fun reverseMap(to: TO): FROM_KEY? {
        return reverseKeyMapper(to)
    }

    val sourceTable: DbTable<FROM, *>
        get() = info.oneTable

    override val targetTable: DbTable<TO, *>
        get() = info.manyTable

    fun createCondition(fromIds: Set<FROM_KEY>, tableInQuery: TableInQuery<TO>): FilterExpr {
        return queryExprBuilder(fromIds, tableInQuery)
    }

    override suspend fun invoke(from: FROM): List<TO> {
        return from.db.load(this, from)
    }

    override suspend fun invoke(from: FROM, block: ScalarExprBuilder<TO>.() -> FilterExpr): List<TO> {
        val query: EntityQuery<TO> = info.manyTable.newQuery(from.db)

        query.filter { createCondition(setOf(info.oneKey(from)), query.table) }
        query.filter(block)

        return query.execute()
    }

    override suspend fun countAll(from: FROM): Long {
        val query: EntityQuery<TO> = info.manyTable.newQuery(from.db)

        query.filter { createCondition(setOf(info.oneKey(from)), query.table) }

        return query.countAll()
    }

    override suspend fun count(from: FROM, block: ScalarExprBuilder<TO>.() -> FilterExpr): Long {
        val query: EntityQuery<TO> = info.manyTable.newQuery(from.db)

        query.filter { createCondition(setOf(info.oneKey(from)), query.table) }
        query.filter(block)

        return query.countAll()
    }

    internal suspend fun callLoadToManyWithFilter(db: DbLoaderImpl, from: FROM, filter: ScalarExprBuilder<TO>.() -> FilterExpr): List<TO> {
        return db.loadToManyWithFilter(from, this, filter)
    }

    override suspend fun loadNow(keys: Set<FROM>, db: DbConn): Map<FROM, List<TO>> {
        val index = keys.associateBy { it.id }

        val query = info.manyTable.newQuery(db)
        query.filter { createCondition(index.keys, table) }
        val result = query.execute()

        val mapped = LinkedHashMap<FROM, ArrayList<TO>>()
        for (to in result) {
            val fromId = reverseMap(to) ?: continue
            val fromEntity = index[fromId] ?: continue
            mapped.computeIfAbsent(fromEntity) { ArrayList() }.add(to)
        }

        return mapped
    }

    override fun nullResult(): List<TO> {
        return emptyList()
    }

    override fun isRelated(table: DbTable<*, *>): Boolean {
        return table == info.oneTable || table == info.manyTable
    }
}
