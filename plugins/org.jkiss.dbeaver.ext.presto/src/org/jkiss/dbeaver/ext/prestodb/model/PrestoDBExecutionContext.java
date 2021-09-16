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
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.generic.model.GenericCatalog;
import org.jkiss.dbeaver.ext.generic.model.GenericExecutionContext;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCExecutionPurpose;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCRemoteInstance;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.runtime.VoidProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLConstants;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.utils.CommonUtils;

import java.sql.SQLException;
import java.util.Objects;

final class PrestoDBExecutionContext extends GenericExecutionContext {
    private static final Log log = Log.getLog(PrestoDBExecutionContext.class);

    @Nullable
    private String catalogName;

    @Nullable
    private String schemaName;

    PrestoDBExecutionContext(@NotNull JDBCRemoteInstance instance, @NotNull String purpose) {
        super(instance, purpose);
    }

    @Override
    public boolean supportsCatalogChange() {
        return true;
    }

    @Override
    public boolean supportsSchemaChange() {
        return true;
    }

    @Override
    public void setDefaultSchema(@NotNull DBRProgressMonitor monitor, @Nullable GenericSchema schema) throws DBCException {
        if (schema == null) {
            return;
        }
        setDefaultCatalog(monitor, schema.getCatalog(), schema);
    }

    @Override
    public void setDefaultCatalog(@NotNull DBRProgressMonitor monitor, @Nullable GenericCatalog catalog, @Nullable GenericSchema schema)
            throws DBCException {
        if (catalog == null || schema == null) {
            return;
        }
        String newCatalogName = catalog.getName();
        String newSchemaName = schema.getName();
        boolean isNewCatalog = !Objects.equals(newCatalogName, catalogName);
        boolean isNewSchema = !Objects.equals(newSchemaName, schemaName);
        if (!isNewCatalog && !isNewSchema) {
            return;
        }

        String sql = "USE \"" + getQuoted(newCatalogName) + SQLConstants.STRUCT_SEPARATOR + getQuoted(newSchemaName);
        try (JDBCSession session = openSession(monitor, DBCExecutionPurpose.UTIL, "Set active catalog and schema")) {
            try (JDBCStatement dbStat = session.createStatement()) {
                dbStat.executeUpdate(sql);
            }
        } catch (SQLException e) {
            log.error("Unable to set active catalog and schema due to unexpected SQLException. " +
                "catalogName=" + newCatalogName + "; schemaName=" + newSchemaName);
            throw new DBCException(e, this);
        }

        if (isNewCatalog) {
            DBSObject oldCatalogInstance = getDefaultCatalog();
            if (oldCatalogInstance != null) {
                DBUtils.fireObjectSelectionChange(oldCatalogInstance, catalog);
            }
            catalogName = newCatalogName;
        }
        if (isNewSchema) {
            DBSObject oldSchemaInstance = getDefaultSchema();
            if (oldSchemaInstance != null) {
                DBUtils.fireObjectSelectionChange(oldSchemaInstance, catalog);
            }
            schemaName = newSchemaName;
        }
    }

    private static String getQuoted(@NotNull String identifier) {
        return SQLConstants.DEFAULT_IDENTIFIER_QUOTE + identifier + SQLConstants.DEFAULT_IDENTIFIER_QUOTE;
    }

    //----- Copy-pasted from SnowflakeExecutionContext. I have no regrets.

    @Nullable
    @Override
    public GenericCatalog getDefaultCatalog() {
        if (CommonUtils.isEmpty(catalogName)) {
            return null;
        }
        return getDataSource().getCatalog(catalogName);
    }

    @Nullable
    @Override
    public GenericSchema getDefaultSchema() {
        GenericCatalog defaultCatalog = getDefaultCatalog();
        if (defaultCatalog == null) {
            return null;
        }
        try {
            return defaultCatalog.getSchema(new VoidProgressMonitor(), schemaName);
        } catch (DBException e) {
            log.error("Unable to retrieve active schema by its name", e);
            return null;
        }
    }

    //-----

    @Override
    public boolean refreshDefaults(DBRProgressMonitor monitor, boolean useBootstrapSettings) {
        return true; // TODO
    }
}
