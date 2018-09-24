package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.crud.EntityQuery
import com.github.mslenc.dbktx.crud.FilterBuilder
import com.github.mslenc.dbktx.expr.ExprBoolean
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.RelToOne
import com.github.mslenc.dbktx.schema.StringColumn

class TextSearchBuilder<E: DbEntity<E, *>>(val entityQuery: EntityQuery<E>, val words: List<String>) {
    val subFilters: MutableList<MutableList<FilterBuilder<E>.()->ExprBoolean>> = ArrayList()
    init {
        for (word in words)
            subFilters.add(ArrayList())
    }

    constructor(entityQuery: EntityQuery<E>, words: String) :
            this(entityQuery, extractWordsForSearch(words))

    fun matchBeginningOf(field: StringColumn<E>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { field startsWith words[i] }

        return this
    }

    fun matchAnywhereIn(field: StringColumn<E>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { field contains words[i] }

        return this
    }

    fun <REF : DbEntity<REF, *>> matchBeginningOf(ref: RelToOne<E, REF>, field: StringColumn<REF>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { field startsWith words[i] } }

        return this
    }

    fun <REF : DbEntity<REF, *>> matchAnywhereIn(ref: RelToOne<E, REF>, field: StringColumn<REF>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { field contains words[i] } }

        return this
    }

    fun <REF : DbEntity<REF, *>, NEXT_REF: DbEntity<NEXT_REF, *>>
            matchBeginningOf(ref: RelToOne<E, REF>, nextRef: RelToOne<REF, NEXT_REF>, field: StringColumn<NEXT_REF>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { nextRef.has { field startsWith words[i] } } }

        return this
    }

    fun <REF : DbEntity<REF, *>, NEXT_REF: DbEntity<NEXT_REF, *>>
            matchAnywhereIn(ref: RelToOne<E, REF>, nextRef: RelToOne<REF, NEXT_REF>, field: StringColumn<NEXT_REF>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { nextRef.has { field contains words[i] } } }

        return this
    }

    fun applyToQuery() {
        // combined = (matches word1) AND (matches word2) AND (...)
        // matches word = (field1 matches word) OR (field2 matches word) OR (...)

        entityQuery.filter {
            ExprBoolean.createAND(
                    subFilters.map { wordFilters ->
                        ExprBoolean.createOR(wordFilters.map { it() })
                    }
            )
        }
    }
}