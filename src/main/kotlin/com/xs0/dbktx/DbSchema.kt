package com.xs0.dbktx

import com.xs0.dbktx.composite.CompositeId
import java.util.*
import kotlin.reflect.KClass

abstract class DbSchema protected constructor() : Iterable<DbTable<*, *>> {
    private val tablesByDbName: LinkedHashMap<String, DbTable<*, *>> = LinkedHashMap()
    private val tablesByClass: LinkedHashMap<KClass<*>, DbTable<*, *>> = LinkedHashMap()

    fun <E : DbEntity<E, ID>, ID: Any> table(dbTableName: String, entityClass: KClass<E>): DbTableBuilder<E, ID> {
        if (tablesByDbName.containsKey(dbTableName))
            throw IllegalArgumentException("Table name $dbTableName is already defined")

        if (tablesByClass.containsKey(entityClass))
            throw IllegalArgumentException("$entityClass is already defined")

        val res = DbTable(this, dbTableName, entityClass, DbTableBuilder.determineIdClass(entityClass))

        tablesByDbName.put(dbTableName, res)
        tablesByClass.put(entityClass, res)

        return DbTableBuilder(res)
    }

    fun <E : DbEntity<E, ID>, ID : CompositeId<E, ID>> tableC(dbTableName: String, entityClass: KClass<E>): DbTableBuilderC<E, ID> {
        if (tablesByDbName.containsKey(dbTableName))
            throw IllegalArgumentException("Table name $dbTableName is already defined")

        if (tablesByClass.containsKey(entityClass))
            throw IllegalArgumentException(entityClass.toString() + " is already defined")

        val res = DbTable(this, dbTableName, entityClass, DbTableBuilder.determineIdClass(entityClass))

        tablesByDbName.put(dbTableName, res)
        tablesByClass.put(entityClass, res)

        return DbTableBuilderC(res)
    }

    @Suppress("UNCHECKED_CAST")
    fun <E : DbEntity<E, ID>, ID: Any> getTableFor(klass: KClass<E>): DbTable<E, ID> {
        return (tablesByClass[klass] ?: throw IllegalArgumentException("No info is available for " + klass))
                as DbTable<E, ID>
    }

    fun getTableFor(tableName: String): DbTable<*, *> {
        return tablesByDbName[tableName] ?: throw IllegalArgumentException("No info is available for table " + tableName)
    }

    override fun iterator(): Iterator<DbTable<*, *>> {
        return Collections.unmodifiableCollection(tablesByDbName.values).iterator()
    }

    private val lazyInits: TreeMap<Int, ArrayList<() -> Unit>> = TreeMap()

    fun finishInit() {
        try {
            for (list in lazyInits.values)
                for (runnable in list)
                    runnable()
        } finally {
            lazyInits.clear()
        }

        // TODO
    }

    fun addLazyInit(priority: Int, initializer: () -> Unit) {
        lazyInits.computeIfAbsent(priority, { _ -> ArrayList() }).add(initializer)
    }

    val numberOfTables: Int
        get() = tablesByDbName.size
}