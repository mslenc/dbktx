package com.xs0.dbktx.util;

import com.xs0.dbktx.crud.BoundColumnForSelect;
import com.xs0.dbktx.crud.TableInQuery;
import com.xs0.dbktx.expr.Expr;
import com.xs0.dbktx.schema.ColumnMapping;

public class SqlBuilderHelpers {
    public static BoundColumnForSelect forceBindColumnFrom(ColumnMapping mapping, TableInQuery table) {
        return mapping.bindColumnFrom(table);
    }

    public static BoundColumnForSelect forceBindColumnTo(ColumnMapping mapping, TableInQuery table) {
        return mapping.bindColumnTo(table);
    }

    public static Expr forceBindFrom(ColumnMapping mapping, TableInQuery table) {
        return mapping.bindFrom(table);
    }
}
