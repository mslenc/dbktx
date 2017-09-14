package com.xs0.dbktx

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.junit.Before;
import org.junit.Test;
import si.datastat.db.schemas.test1.Brand;
import si.datastat.db.schemas.test1.Company;
import si.datastat.db.schemas.test1.Item;
import si.datastat.db.schemas.test1.TestSchema1;
import si.datastat.db.testutils.DelayedExec;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static si.datastat.db.schemas.test1.TestSchema1.ITEM;

public class DbLoaderTest {
    @Before
    public void loadSchema() {
        assertNotNull(TestSchema1.Companion.getTEST1());
    }

    private void checkParams(JsonArray array, Object... expected) {
        HashMap<Object, Integer> exp = new LinkedHashMap<>();
        for (Object e : expected)
            exp.compute(e, (a, b) -> b == null ? 1 : 1 + b);

        HashMap<Object, Integer> actual = new LinkedHashMap<>();
        for (Object e : array.getList())
            actual.compute(e, (a, b) -> b == null ? 1 : 1 + b);

        assertEquals(exp, actual);
    }

    private static JsonArray array(Object... values) {
        return new JsonArray(asList(values));
    }


    @Test
    public void testBatchedLoadingEntities() {
        SQLConnection conn = Mockito.mock(SQLConnection.class);
        DelayedExec delayedExec = new DelayedExec();
        DbConn<Void, Void> loader = new DbLoaderImpl<>(conn, null, delayedExec);

        @SuppressWarnings("unchecked")
        AsyncResult<Item>[] results = new AsyncResult[4];

        Item.Id id0 = new Item.Id("abc", 123L);
        Item.Id id1 = new Item.Id("def", 123L);
        Item.Id id2 = new Item.Id("ghi", 234L);
        Item.Id id3 = new Item.Id("jkl", 234L);

        loader.load(Companion.getITEM(), id2, result -> results[2] = result);
        loader.load(Companion.getITEM(), id0, result -> results[0] = result);
        loader.load(Companion.getITEM(), id1, result -> results[1] = result);
        loader.load(Companion.getITEM(), id3, result -> results[3] = result);

        Mockito.verifyZeroInteractions(conn);
        assertNull(results[0]);
        assertNull(results[1]);
        assertNull(results[2]);
        assertNull(results[3]);

        Mockito.when(conn.queryWithParams(ArgumentMatchers.anyString(), ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any())).then(invocation -> {
            String sql = invocation.getArgument(0);
            JsonArray params = invocation.getArgument(1);
            Handler<AsyncResult<ResultSet>> resultHandler = invocation.getArgument(2);

            assertEquals("SELECT company_id, sku, brand_key, name, price, t_created, t_updated FROM items WHERE (sku, company_id) IN ((?, ?), (?, ?), (?, ?), (?, ?))", sql);

            checkParams(params, id0.getPartA(), id0.getPartB(),
                                id1.getPartA(), id1.getPartB(),
                                id2.getPartA(), id2.getPartB(),
                                id3.getPartA(), id3.getPartB());

            resultHandler.handle(Future.succeededFuture(new ResultSet(
                asList("company_id", "sku", "brand_key", "name", "price", "t_created", "t_updated"),
                asList(
                    array(123L, "abc", "bk1", "Item 1", "123.45", "2017-06-27", "2017-06-27"),
                    array(123L, "def", "bk2", "Item 2", "432.45", "2017-06-26", "2017-06-27"),
                    array(234L, "ghi", "bk3", "Item 3", "500.45", "2017-06-25", "2017-06-27")
                ),
                null
            )));

            return null;
        });

        delayedExec.executePending();

        assertNotNull(results[0]);
        assertNotNull(results[1]);
        assertNotNull(results[2]);
        assertNotNull(results[3]);

        assertTrue(results[0].succeeded());
        assertTrue(results[1].succeeded());
        assertTrue(results[2].succeeded());
        assertTrue(results[3].succeeded());

        assertEquals(id0, results[0].result().getID());
        assertEquals(id1, results[1].result().getID());
        assertEquals(id2, results[2].result().getID());
        assertNull(results[3].result());
    }

    @Test
    public void testBatchedLoadingToMany() {
        SQLConnection conn = Mockito.mock(SQLConnection.class);
        DelayedExec delayedExec = new DelayedExec();
        DbConn<Void, Void> loader = new DbLoaderImpl<>(conn, null, delayedExec);

        Long comId0 = 412L;
        Long comId1 = 314L;
        Long comId2 = 541515L;

        assertEquals("id, name, t_created, t_updated", TestSchema1.Companion.getCOMPANY().columnNames());

        Company com0 = new Company(comId0, asList(comId0, "company",      "2017-06-01", "2017-06-13"));
        Company com1 = new Company(comId1, asList(comId1, "corporation",  "2017-06-02", "2017-06-12"));
        Company com2 = new Company(comId2, asList(comId2, "organization", "2017-06-03", "2017-06-11"));

        @SuppressWarnings("unchecked")
        AsyncResult<List<Brand>>[] results = new AsyncResult[3];

        loader.load(com2, Company.Companion.getBRANDS_SET(), result -> results[2] = result);
        loader.load(com0, Company.Companion.getBRANDS_SET(), result -> results[0] = result);
        loader.load(com1, Company.Companion.getBRANDS_SET(), result -> results[1] = result);

        Mockito.verifyZeroInteractions(conn);
        assertNull(results[0]);
        assertNull(results[1]);
        assertNull(results[2]);

        Mockito.when(conn.queryWithParams(ArgumentMatchers.anyString(), ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any())).then(invocation -> {
            String sql = invocation.getArgument(0);
            JsonArray params = invocation.getArgument(1);
            Handler<AsyncResult<ResultSet>> resultHandler = invocation.getArgument(2);

            assertEquals("SELECT company_id, key, name, tag_line, t_created, t_updated FROM brands WHERE company_id IN (?, ?, ?)", sql);

            checkParams(params, comId0, comId1, comId2);

            resultHandler.handle(Future.succeededFuture(new ResultSet(
                    asList("company_id", "key", "name", "tag_line", "t_created", "t_updated"),
                    asList(
                        array(comId1, "abc", "Abc (tm)",     "We a-b-c for you!", "2017-04-27", "2017-05-27"),
                        array(comId2, "baa", "Sheeps Inc.",  "Wool and stool!",   "2017-02-25", "2017-03-27"),
                        array(comId1, "goo", "Gooey Phooey", "Tee hee mee bee",   "2017-03-26", "2017-04-27")
                    ),
                    null
            )));

            return null;
        });

        delayedExec.executePending();

        assertNotNull(results[0]);
        assertNotNull(results[1]);
        assertNotNull(results[2]);

        assertTrue(results[0].succeeded());
        assertTrue(results[1].succeeded());
        assertTrue(results[2].succeeded());

        assertEquals(0, results[0].result().size());
        assertEquals(2, results[1].result().size());
        assertEquals(1, results[2].result().size());

        // TODO: check some props as well
    }

    @Test
    public void testBatchedLoadingToManyMultiField() {
        SQLConnection conn = Mockito.mock(SQLConnection.class);
        DelayedExec delayedExec = new DelayedExec();
        DbLoaderImpl<Void, Void> loader = new DbLoaderImpl<>(conn, null, delayedExec);

        Brand.Id id0 = new Brand.Id("abc", 1024L);
        Brand.Id id1 = new Brand.Id("baa",  256L);
        Brand.Id id2 = new Brand.Id("goo",   16L);

        assertEquals("company_id, key, name, tag_line, t_created, t_updated", TestSchema1.Companion.getBRAND().columnNames());

        Brand brand0 = new Brand(id0, asList(1024L, "abc", "Abc (tm)",     "We a-b-c for you!", "2017-04-27", "2017-05-27"));
        Brand brand1 = new Brand(id1, asList( 256L, "baa", "Sheeps Inc.",  "Wool and stool!",   "2017-02-25", "2017-03-27"));
        Brand brand2 = new Brand(id2, asList(  16L, "goo", "Gooey Phooey", "Tee hee mee bee",   "2017-03-26", "2017-04-27"));

        @SuppressWarnings("unchecked")
        AsyncResult<List<Item>>[] results = new AsyncResult[3];

        loader.load(brand1, Brand.Companion.getITEMS_SET(), result -> results[1] = result);
        loader.load(brand2, Brand.Companion.getITEMS_SET(), result -> results[2] = result);
        loader.load(brand0, Brand.Companion.getITEMS_SET(), result -> results[0] = result);

        Mockito.verifyZeroInteractions(conn);
        assertNull(results[0]);
        assertNull(results[1]);
        assertNull(results[2]);

        Mockito.when(conn.queryWithParams(ArgumentMatchers.anyString(), ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any())).then(invocation -> {
            String sql = invocation.getArgument(0);
            JsonArray params = invocation.getArgument(1);
            Handler<AsyncResult<ResultSet>> resultHandler = invocation.getArgument(2);

            assertEquals("SELECT company_id, sku, brand_key, name, price, t_created, t_updated FROM items WHERE (brand_key, company_id) IN ((?, ?), (?, ?), (?, ?))", sql);

            checkParams(params, id0.getPartA(), id0.getPartB(),
                                id1.getPartA(), id1.getPartB(),
                                id2.getPartA(), id2.getPartB());

            resultHandler.handle(Future.succeededFuture(new ResultSet(
                    asList("company_id", "key", "name", "tag_line", "t_created", "t_updated"),
                    asList(
                        array(id1.getPartB(), "SHP001", id1.getPartA(), "A white sheep",       "412.50", "2017-04-27", "2017-05-27"),
                        array(id1.getPartB(), "SHP010", id1.getPartA(), "A black sheep",       "999.95", "2017-03-27", "2017-04-27"),
                        array(id1.getPartB(), "TOO001", id1.getPartA(), "A fine wool trimmer", "111.11", "2017-04-27", "2017-05-27"),
                        array(id2.getPartB(), "GOO",    id2.getPartA(), "The Goo",               "4.50", "2016-01-01", "2016-01-01")
                    ),
                    null
            )));

            return null;
        });

        delayedExec.executePending();

        assertNotNull(results[0]);
        assertNotNull(results[1]);
        assertNotNull(results[2]);

        assertTrue(results[0].succeeded());
        assertTrue(results[1].succeeded());
        assertTrue(results[2].succeeded());

        assertEquals(0, results[0].result().size());
        assertEquals(3, results[1].result().size());
        assertEquals(1, results[2].result().size());

        // TODO: check some props as well
    }
}