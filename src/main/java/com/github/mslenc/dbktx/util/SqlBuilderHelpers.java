package com.github.mslenc.dbktx.util;

import com.github.mslenc.dbktx.crud.BoundColumnForSelect;
import com.github.mslenc.dbktx.crud.TableInQuery;
import com.github.mslenc.dbktx.expr.Expr;
import com.github.mslenc.dbktx.schema.ColumnMapping;

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
