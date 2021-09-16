/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2021 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.prestodb.model;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericSQLDialect;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCExecutionContext;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCRemoteInstance;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.utils.CommonUtils;

final class PrestoDBDataSource extends GenericDataSource {
    PrestoDBDataSource(@NotNull DBRProgressMonitor monitor, @NotNull DBPDataSourceContainer container,
                       @NotNull GenericMetaModel metaModel) throws DBException {
        super(monitor, container, metaModel, new GenericSQLDialect());
    }

    @NotNull
    @Override
    protected JDBCExecutionContext createExecutionContext(@NotNull JDBCRemoteInstance instance, @NotNull String type) {
        return new PrestoDBExecutionContext(instance, type);
    }

    @Override
    protected void initializeContextState(@NotNull DBRProgressMonitor monitor, @NotNull JDBCExecutionContext context, 
                                          @Nullable JDBCExecutionContext initFrom) throws DBCException {
        PrestoDBExecutionContext ctx = CommonUtils.tryCast(context, PrestoDBExecutionContext.class);
        if (ctx == null) {
            return;
        }
        PrestoDBExecutionContext metaCtx = CommonUtils.tryCast(initFrom, PrestoDBExecutionContext.class);
        if (metaCtx != null) {
            ctx.setDefaultCatalog(monitor, metaCtx.getDefaultCatalog(), metaCtx.getDefaultSchema());
        } else {
            ctx.refreshDefaults(monitor, true);
        }
    }
}
