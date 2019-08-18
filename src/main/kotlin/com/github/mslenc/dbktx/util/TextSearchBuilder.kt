package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.crud.EntityQuery
import com.github.mslenc.dbktx.crud.FilterBuilder
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.filters.FilterAnd
import com.github.mslenc.dbktx.filters.FilterOr
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.RelToSingle
import com.github.mslenc.dbktx.schema.StringColumn

class TextSearchBuilder<E: DbEntity<E, *>>(val entityQuery: EntityQuery<E>, val words: List<String>) {
    val subFilters: MutableList<MutableList<FilterBuilder<E>.()->FilterExpr>> = ArrayList()
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

    fun <REF : DbEntity<REF, *>> matchBeginningOf(ref: RelToSingle<E, REF>, field: StringColumn<REF>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { field startsWith words[i] } }

        return this
    }

    fun <REF : DbEntity<REF, *>> matchAnywhereIn(ref: RelToSingle<E, REF>, field: StringColumn<REF>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { field contains words[i] } }

        return this
    }

    fun <REF : DbEntity<REF, *>, NEXT_REF: DbEntity<NEXT_REF, *>>
    matchBeginningOf(ref: RelToSingle<E, REF>, nextRef: RelToSingle<REF, NEXT_REF>, field: StringColumn<NEXT_REF>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { nextRef.has { field startsWith words[i] } } }

        return this
    }

    fun <REF : DbEntity<REF, *>, NEXT_REF: DbEntity<NEXT_REF, *>>
    matchAnywhereIn(ref: RelToSingle<E, REF>, nextRef: RelToSingle<REF, NEXT_REF>, field: StringColumn<NEXT_REF>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { nextRef.has { field contains words[i] } } }

        return this
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
    matchBeginningOf(ref: RelToSingle<E, REF>, ref2: RelToSingle<REF, REF2>, ref3: RelToSingle<REF2, REF3>, field: StringColumn<REF3>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { ref2.has { ref3.has { field startsWith words[i] } } } }

        return this
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
    matchAnywhereIn(ref: RelToSingle<E, REF>, ref2: RelToSingle<REF, REF2>, ref3: RelToSingle<REF2, REF3>, field: StringColumn<REF3>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { ref2.has { ref3.has { field contains words[i] } } } }

        return this
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
    matchBeginningOf(ref: RelToSingle<E, REF>, ref2: RelToSingle<REF, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, field: StringColumn<REF4>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { ref2.has { ref3.has { ref4.has { field startsWith words[i] } } } } }

        return this
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
    matchAnywhereIn(ref: RelToSingle<E, REF>, ref2: RelToSingle<REF, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, field: StringColumn<REF4>): TextSearchBuilder<E> {
        for (i in 0 until words.size)
            subFilters[i].add { ref.has { ref2.has { ref3.has { ref4.has { field contains words[i] } } } } }

        return this
    }

    fun applyToQuery() {
        // combined = (matches word1) AND (matches word2) AND (...)
        // matches word = (field1 matches word) OR (field2 matches word) OR (...)

        entityQuery.filter {
            FilterAnd.create(
                subFilters.map { wordFilters ->
                    FilterOr.create(wordFilters.map { it() })
                }
            )
        }
    }
}