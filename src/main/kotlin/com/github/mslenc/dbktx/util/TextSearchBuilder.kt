package com.github.mslenc.dbktx.util

import com.github.mslenc.dbktx.crud.FilterableQuery
import com.github.mslenc.dbktx.crud.createFilter
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.expr.FilterExprBuilder
import com.github.mslenc.dbktx.filters.FilterAnd
import com.github.mslenc.dbktx.filters.FilterOr
import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.Rel

private class WordGroup(val words: List<String>) {
    val subFilters: MutableList<MutableList<Expr<Boolean>>> = ArrayList()
    init {
        repeat(words.size) {
            subFilters.add(ArrayList())
        }
    }

    inline fun forEachWord(block: (String)->Expr<Boolean>) {
        for (i in words.indices) {
            subFilters[i].add(block(words[i]))
        }
    }
}



class TextSearchBuilder<E: DbEntity<E, *>>(val query: FilterableQuery<E>, wordGroups: List<List<String>>) {
    private val groups = wordGroups.mapNotNull {
        when {
            it.isNotEmpty() -> WordGroup(it)
            else -> null
        }
    }

    private inline fun List<WordGroup>.addWordMatchers(block: FilterExprBuilder<E>.(String) -> Expr<Boolean>): TextSearchBuilder<E> {
        forEach { group ->
            group.forEachWord {
                query.createFilter { block(it) }
            }
        }
        return this@TextSearchBuilder
    }

    fun matchBeginningOf(field: Column<E, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            field istartsWith it
        }
    }

    fun matchAnywhereIn(field: Column<E, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            field icontains it
        }
    }

    fun matchExactly(field: Column<E, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            bind(field) eq field.makeLiteral(it)
        }
    }

    fun <REF : DbEntity<REF, *>> matchBeginningOf(ref: Rel<E, REF>, field: Column<REF, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { field istartsWith it }
        }
    }

    fun <REF : DbEntity<REF, *>> matchAnywhereIn(ref: Rel<E, REF>, field: Column<REF, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { field icontains it }
        }
    }

    fun <REF : DbEntity<REF, *>> matchExactly(ref: Rel<E, REF>, field: Column<REF, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { bind(field) eq field.makeLiteral(it) }
        }
    }

    fun <REF : DbEntity<REF, *>, NEXT_REF: DbEntity<NEXT_REF, *>>
    matchBeginningOf(ref: Rel<E, REF>, nextRef: Rel<REF, NEXT_REF>, field: Column<NEXT_REF, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { nextRef.matches { field istartsWith it } }
        }
    }

    fun <REF : DbEntity<REF, *>, NEXT_REF: DbEntity<NEXT_REF, *>>
    matchAnywhereIn(ref: Rel<E, REF>, nextRef: Rel<REF, NEXT_REF>, field: Column<NEXT_REF, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { nextRef.matches { field icontains it } }
        }
    }

    fun <REF : DbEntity<REF, *>, NEXT_REF: DbEntity<NEXT_REF, *>>
    matchExactly(ref: Rel<E, REF>, nextRef: Rel<REF, NEXT_REF>, field: Column<NEXT_REF, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { nextRef.matches { bind(field) eq field.makeLiteral(it) } }
        }
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
    matchBeginningOf(ref: Rel<E, REF>, ref2: Rel<REF, REF2>, ref3: Rel<REF2, REF3>, field: Column<REF3, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { ref2.matches { ref3.matches { field istartsWith it } } }
        }
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
    matchAnywhereIn(ref: Rel<E, REF>, ref2: Rel<REF, REF2>, ref3: Rel<REF2, REF3>, field: Column<REF3, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { ref2.matches { ref3.matches { field icontains it } } }
        }
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
    matchExactly(ref: Rel<E, REF>, ref2: Rel<REF, REF2>, ref3: Rel<REF2, REF3>, field: Column<REF3, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { ref2.matches { ref3.matches { bind(field) eq field.makeLiteral(it) } } }
        }
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
    matchBeginningOf(ref: Rel<E, REF>, ref2: Rel<REF, REF2>, ref3: Rel<REF2, REF3>, ref4: Rel<REF3, REF4>, field: Column<REF4, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { ref2.matches { ref3.matches { ref4.matches { field istartsWith it } } } }
        }
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
    matchAnywhereIn(ref: Rel<E, REF>, ref2: Rel<REF, REF2>, ref3: Rel<REF2, REF3>, ref4: Rel<REF3, REF4>, field: Column<REF4, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { ref2.matches { ref3.matches { ref4.matches { field icontains it } } } }
        }
    }

    fun <REF : DbEntity<REF, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
    matchExactly(ref: Rel<E, REF>, ref2: Rel<REF, REF2>, ref3: Rel<REF2, REF3>, ref4: Rel<REF3, REF4>, field: Column<REF4, String>): TextSearchBuilder<E> {
        return groups.addWordMatchers {
            ref.matches { ref2.matches { ref3.matches { ref4.matches { bind(field) eq field.makeLiteral(it) } } } }
        }
    }

    fun createFilterExpr(): Expr<Boolean> {
        // combined = (group1) OR (group2) OR (...)
        // group = (match word1) AND (match word2) AND (...)
        // match word = (field1 matches word) OR (field2 matches word) OR (...)

        return (
            FilterOr.create(
                groups.map { group ->
                    FilterAnd.create(
                        group.subFilters.map { wordFilters ->
                            FilterOr.create(wordFilters)
                        }
                    )
                }
            )
        )
    }

    fun applyToQuery() {
        query.require(createFilterExpr())
    }

    companion object {
        operator fun <E: DbEntity<E, *>> invoke(query: FilterableQuery<E>, words: String): TextSearchBuilder<E> {
            val wordGroups = words.split(',').mapNotNull {
                val wordsFound = extractWordsForSearch(it)
                when {
                    wordsFound.isEmpty() -> null
                    else -> wordsFound
                }
            }

            return TextSearchBuilder(query, wordGroups)
        }
    }
}