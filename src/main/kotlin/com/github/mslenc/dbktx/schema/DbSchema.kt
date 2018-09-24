package com.github.mslenc.dbktx.schema

import java.util.*
import kotlin.reflect.KClass

abstract class DbSchema protected constructor() : Iterable<DbTable<*, *>> {
    private val tablesByDbName: LinkedHashMap<String, DbTable<*, *>> = LinkedHashMap()
    private val tablesByClass: LinkedHashMap<KClass<*>, DbTable<*, *>> = LinkedHashMap()

    fun <E: DbEntity<E, ID>, ID: Any> register(table: DbTable<E, ID>) {
        if (tablesByDbName.containsKey(table.dbName))
            throw IllegalArgumentException("Table name ${table.dbName} is already defined")

        if (tablesByClass.containsKey(table.entityClass))
            throw IllegalArgumentException("${table.entityClass} is already defined")

        tablesByDbName.put(table.dbName, table)
        tablesByClass.put(table.entityClass, table)
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