/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query.util;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

/**
 * Normalize a Doris SQL subset into syntax that can be parsed by Calcite.
 * This transformer is guarded by config and falls back to original SQL on unsupported patterns.
 */
public class DorisSqlDialectConverter implements IQueryTransformer, IPushDownConverter {

    private static final Pattern DORIS_HINT_PATTERN = Pattern.compile("(?is)/\\*\\+.*?\\*/");
    private static final Pattern IFNULL_PATTERN = Pattern.compile("(?i)\\bifnull\\s*\\(");
    private static final Pattern NVL_PATTERN = Pattern.compile("(?i)\\bnvl\\s*\\(");

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        return convert(sql, project, defaultSchema);
    }
    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        if (StringUtils.isBlank(originSql)) {
            return originSql;
        }
        if (!KylinConfig.getInstanceFromEnv().isDorisSqlDialectTransformEnabled()) {
            return originSql;
        }
        return normalizeDorisSql(originSql);
    }

    static String normalizeDorisSql(String sql) {
        if (StringUtils.isBlank(sql)) {
            return sql;
        }
        String normalized = DORIS_HINT_PATTERN.matcher(sql).replaceAll(" ");
        normalized = StringHelper.backtickToDoubleQuote(normalized);
        normalized = IFNULL_PATTERN.matcher(normalized).replaceAll("COALESCE(");
        normalized = NVL_PATTERN.matcher(normalized).replaceAll("COALESCE(");
        return normalized;
    }
}
