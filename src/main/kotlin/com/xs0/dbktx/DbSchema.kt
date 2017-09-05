package com.xs0.dbktx

import si.datastat.db.api.DbEntity
import si.datastat.db.api.DbTableBuilder
import si.datastat.db.api.DbTableBuilderC
import si.datastat.db.api.props.Collate
import si.datastat.db.api.str.DbCollation
import si.datastat.db.api.util.CompositeId
import java.util.*
import kotlin.reflect.KClass

abstract class DbSchema protected constructor(val defaultCollation: DbCollation) : Iterable<DbTable<*, *>> {
    private val tablesByDbName: LinkedHashMap<String, DbTable<*, *>> = LinkedHashMap()
    private val tablesByClass: LinkedHashMap<KClass<*>, DbTable<*, *>> = LinkedHashMap()

    fun <E : DbEntity<E, ID>, ID> table(dbTableName: String, entityClass: KClass<E>): DbTableBuilder<E, ID> {
        return table(dbTableName, entityClass, null)
    }

    fun <E : DbEntity<E, ID>, ID> table(dbTableName: String, entityClass: KClass<E>, defaultCollation: Collate?): DbTableBuilder<E, ID> {
        if (tablesByDbName.containsKey(dbTableName))
            throw IllegalArgumentException("Table name $dbTableName is already defined")

        if (tablesByClass.containsKey(entityClass))
            throw IllegalArgumentException("$entityClass is already defined")

        val res = DbTable(this, dbTableName, entityClass, DbTableBuilder.determineIdClass(entityClass), defaultCollation?.collation)

        tablesByDbName.put(dbTableName, res)
        tablesByClass.put(entityClass, res)

        return DbTableBuilder(res)
    }

    fun <E : DbEntity<E, ID>, ID : CompositeId<E, ID>> tableC(dbTableName: String, entityClass: Class<E>): DbTableBuilderC<E, ID> {
        return tableC(dbTableName, entityClass, null)
    }

    fun <E : DbEntity<E, ID>, ID : CompositeId<E, ID>> tableC(dbTableName: String, entityClass: Class<E>, defaultCollation: Collate?): DbTableBuilderC<E, ID> {
        if (tablesByDbName.containsKey(dbTableName))
            throw IllegalArgumentException("Table name $dbTableName is already defined")

        if (tablesByClass.containsKey(entityClass))
            throw IllegalArgumentException(entityClass.toString() + " is already defined")

        val res = DbTable(this, dbTableName, entityClass, DbTableBuilder.determineIdClass(entityClass), defaultCollation?.collation)

        tablesByDbName.put(dbTableName, res)
        tablesByClass.put(entityClass, res)

        return DbTableBuilderC(res)
    }

    fun <ID, E : DbEntity<E, ID>> getTableFor(klass: Class<E>): DbTable<E, ID> {

        return tablesByClass[klass] as DbTable<E, ID> ?: throw IllegalArgumentException("No info is available for " + klass)
    }

    fun getTableFor(tableName: String): DbTable<*, *> {

        return tablesByDbName[tableName] ?: throw IllegalArgumentException("No info is available for table " + tableName)
    }

    override fun iterator(): Iterator<DbTable<*, *>> {
        return Collections.unmodifiableCollection(tablesByDbName.values).iterator()
    }

    private var lazyInits: TreeMap<Int, ArrayList<Runnable>>? = TreeMap()

    fun finishInit() {
        try {
            for (list in lazyInits!!.values)
                for (runnable in list)
                    runnable.run()
        } finally {
            lazyInits = null
        }

        // TODO
    }

    fun addLazyInit(priority: Int, initializer: Runnable) {
        (lazyInits as java.util.Map<Int, ArrayList<Runnable>>).computeIfAbsent(priority) { p -> ArrayList() }.add(initializer)
    }

    val numberOfTables: Int
        get() = tablesByDbName.size
}