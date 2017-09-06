package com.xs0.dbktx;

import java.util.HashMap;
import java.util.LinkedHashMap;

public abstract class DbMutationImpl<E extends DbEntity<E, ID>, ID> implements DbMutation<E> {
    protected final HashMap<Column<E, ?>, Expr<? super E, ?>> values = new LinkedHashMap<>();
    protected final DbTable<E, ID> table;
    protected final DbConn<?> db;

    protected DbMutationImpl(DbConn<?> db, DbTable<E, ID> table) {
        this.table = table;
        this.db = db;
    }

    public <T> DbMutationImpl<E, ID> set(Column<E, T> column, Expr<? super E, T> value) {
        values.put(column, value);
        return this;
    }

    @Override
    public <TARGET extends DbEntity<TARGET, TID>, TID>
    DbMutation<E> set(RelToOne<E, TARGET> relation, TARGET target) {
        RelToOneImpl<E, ID, TARGET, TID> rel = (RelToOneImpl<E, ID, TARGET, TID>) relation;
        ManyToOneInfo<E, ID, TARGET, TID> info = rel.getInfo();

        for (ColumnMapping<E, TARGET, ?> colMap : info.getColumnMappings()) {
            doColMap(colMap, target);
        }

        return this;
    }

    private <TARGET extends DbEntity<TARGET, TID>, TID, VALTYPE>
    void doColMap(ColumnMapping<E, TARGET, VALTYPE> colMap, TARGET target) {
        set(colMap.getColumnFrom(), colMap.getColumnTo().from(target));
    }


}