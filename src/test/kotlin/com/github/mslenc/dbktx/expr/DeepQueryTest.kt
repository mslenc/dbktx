package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.runMysqlTest
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test4.*
import com.github.mslenc.dbktx.util.NoSuchEntity
import com.github.mslenc.dbktx.util.getContextDb
import com.github.mslenc.utils.smap
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
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
        db.async { db.loadById(DbUser, 825) }.await()
    }

    @Test(expected = NoSuchEntity::class)
    fun testMissingEntity3() = runMysqlTest { db ->
        db.loadById(DbUser, 825)
    }

    @Test(expected = NoSuchEntity::class)
    fun testMissingEntity4() = runMysqlTest { db ->
        db.async(start = CoroutineStart.UNDISPATCHED) { db.loadById(DbUser, 825) }.await()
    }

    @Test
    fun testDeepQuery1() = runMysqlTest { db ->
        db.loadById(DbPerson, 1)
        db.loadById(DbOffer, 300)
        db.loadById(DbOffer, 301)

        listOf<Deferred<*>>(
            db.async { doPersonInfo(db.loadById(DbPerson, 1)) },
            db.async { doOfferInfo(db.loadById(DbOffer, 300)) },
            db.async { doOfferInfo(db.loadById(DbOffer, 300)) }
        ).map { it.await() }
    }

    suspend fun doPersonInfo(person: DbPerson, db: DbConn = getContextDb()) = with(db) {
        listOf(
            async { person.id },
            async { person.firstName },
            async { person.lastName },
            async { person.offers().smap { doOfferInfo(it) } },
            async { person.tickets().smap { doTicketInfo(it) } }
        ).map { it.await() }
    }

    suspend fun doSimplePersonInfo(person: DbPerson, db: DbConn = getContextDb()) = with(db) {
        listOf(
            async { person.id },
            async { person.firstName },
            async { person.lastName }
        ).map { it.await() }
    }

    suspend fun doOfferInfo(offer: DbOffer, db: DbConn = getContextDb()) = with(db) {
        listOf(
            async { offer.id },
            async { doSimplePersonInfo(offer.person()) },
            async { offer.lines().smap { doLineItem(it) } }
        ).map { it.await() }
    }

    suspend fun doLineItem(offerLine: DbOfferLine, db: DbConn = getContextDb()) = with(db) {
        listOf(
            async { offerLine.id },
            async { doProductInfo(offerLine.product()) },
            async { offerLine.tickets().smap { doTicketInfo(it) } }
        ).map { it.await() }
    }

    suspend fun doProductInfo(product: DbProduct, db: DbConn = getContextDb()) = with(db) {
        listOf(
            async { product.id },
            async { product.name }
        ).map { it.await() }
    }

    suspend fun doTicketInfo(ticket: DbTicket, db: DbConn = getContextDb()) = with(db) {
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