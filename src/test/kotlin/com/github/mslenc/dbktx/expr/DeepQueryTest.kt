package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.runMysqlTest
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test4.*
import com.github.mslenc.dbktx.util.NoSuchEntity
import com.github.mslenc.utils.smap
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import org.junit.Test

class DeepQueryTest {
    init {
        initSchemas()
    }

    @Test(expected = NoSuchEntity::class)
    fun testMissingEntity() = runMysqlTest { db ->
        db.loadById(DbUser, 825)
    }

    @Test(expected = NoSuchEntity::class)
    fun testMissingEntity2() = runMysqlTest { db ->
        db.scope.async { db.loadById(DbUser, 825) }.await()
    }

    @Test(expected = NoSuchEntity::class)
    fun testMissingEntity3() = runMysqlTest { db ->
        withContext(db.scope.coroutineContext) { db.loadById(DbUser, 825) }
    }

    @Test(expected = NoSuchEntity::class)
    fun testMissingEntity4() = runMysqlTest { db ->
        db.scope.async(start = CoroutineStart.UNDISPATCHED) { db.loadById(DbUser, 825) }.await()
    }

    @Test
    fun testDeepQuery1() = runMysqlTest { db ->
        db.loadById(DbPerson, 1)
        db.loadById(DbOffer, 300)
        db.loadById(DbOffer, 301)

        listOf(
            db.scope.async { doPersonInfo(db.loadById(DbPerson, 1)) },
            db.scope.async { doOfferInfo(db.loadById(DbOffer, 300)) },
            db.scope.async { doOfferInfo(db.loadById(DbOffer, 300)) }
        ).map { it.await() }
    }

    suspend fun doPersonInfo(person: DbPerson) = with(person.db.scope) {
        listOf(
            async { person.id },
            async { person.firstName },
            async { person.lastName },
            async { person.offers().smap { doOfferInfo(it) } },
            async { person.tickets().smap { doTicketInfo(it) } }
        ).map { it.await() }
    }

    suspend fun doSimplePersonInfo(person: DbPerson) = with(person.db.scope) {
        listOf(
            async { person.id },
            async { person.firstName },
            async { person.lastName }
        ).map { it.await() }
    }

    suspend fun doOfferInfo(offer: DbOffer) = with(offer.db.scope) {
        listOf(
            async { offer.id },
            async { doSimplePersonInfo(offer.person()) },
            async { offer.lines().smap { doLineItem(it) } }
        ).map { it.await() }
    }

    suspend fun doLineItem(offerLine: DbOfferLine) = with(offerLine.db.scope) {
        listOf(
            async { offerLine.id },
            async { doProductInfo(offerLine.product()) },
            async { offerLine.tickets().smap { doTicketInfo(it) } }
        ).map { it.await() }
    }

    suspend fun doProductInfo(product: DbProduct) = with(product.db.scope) {
        listOf(
            async { product.id },
            async { product.name }
        ).map { it.await() }
    }

    suspend fun doTicketInfo(ticket: DbTicket) = with(ticket.db.scope) {
        listOf(
            async { ticket.id },
            async { ticket.person()?.id },
            async {
                ticket.offerLine()?.let { offerLine ->
                    listOf(
                        async { doProductInfo(offerLine.product()) }
                    ).map { it.await() }
                }
            }
        ).map { it.await() }
    }
}