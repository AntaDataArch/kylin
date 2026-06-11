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

import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DorisMvRewriteTransformerTest {

    @Test
    void testDorisSqlDialectNormalization() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        config.setProperty("kylin.query.doris-sql-transform-enabled", "true");
        DorisSqlDialectConverter converter = new DorisSqlDialectConverter();
        String originSql = "select /*+ SET_VAR(query_timeout=3) */ ifnull(`db`.`fact`, 0), nvl(`db`.`v`, 1) from `db`.`fact`";

        try (KylinConfig.SetAndUnsetThreadLocalConfig ignored = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            String rewritten = converter.convert(originSql, "", "default");
            Assertions.assertEquals("select COALESCE(\"db\".\"fact\", 0), COALESCE(\"db\".\"v\", 1) from \"db\".\"fact\"",
                    trimAndCompactWhitespaces(rewritten));
        }
    }
    @Test
    void testRewriteSelectsBestCandidate() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        config.setProperty("kylin.query.doris-mv-rewrite-enabled", "true");
        config.setProperty("kylin.query.doris-mv-rewrite-metadata-json", "["
                + "{\"mvTable\":\"db.mv_old\",\"factTable\":\"db.fact\",\"coveredTables\":[\"db.fact\"],"
                + "\"dimensions\":[\"dt\"],\"measures\":[\"sum\"],\"refreshTime\":1000,\"rowCount\":10000},"
                + "{\"mvTable\":\"db.mv_new\",\"factTable\":\"db.fact\",\"coveredTables\":[\"db.fact\"],"
                + "\"dimensions\":[\"dt\"],\"measures\":[\"sum\"],\"refreshTime\":2000,\"rowCount\":20000}]");

        DorisMvRewriteTransformer transformer = new DorisMvRewriteTransformer();
        String originSql = "select dt, sum(price) from db.fact group by dt";
        try (KylinConfig.SetAndUnsetThreadLocalConfig ignored = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            String rewritten = transformer.transform(originSql, "", "default");
            Assertions.assertEquals("select dt, sum(price) from db.mv_new group by dt", rewritten);
        }
    }

    @Test
    void testRewriteFallbackOnUnsupportedSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        config.setProperty("kylin.query.doris-mv-rewrite-enabled", "true");
        config.setProperty("kylin.query.doris-mv-rewrite-metadata-json",
                "[{\"mvTable\":\"db.mv_new\",\"factTable\":\"db.fact\",\"coveredTables\":[\"db.fact\"],"
                        + "\"dimensions\":[\"dt\"],\"measures\":[\"sum\"],\"refreshTime\":2000}]");

        DorisMvRewriteTransformer transformer = new DorisMvRewriteTransformer();
        String originSql = "select sum(price) over(partition by dt) from db.fact";
        try (KylinConfig.SetAndUnsetThreadLocalConfig ignored = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            String rewritten = transformer.transform(originSql, "", "default");
            Assertions.assertEquals(originSql, rewritten);
        }
    }

    @Test
    void testRewriteFallbackOnSafetyCheck() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        config.setProperty("kylin.query.doris-mv-rewrite-enabled", "true");
        config.setProperty("kylin.query.doris-mv-rewrite-metadata-json",
                "[{\"mvTable\":\"db.mv_new\",\"factTable\":\"db.fact\",\"coveredTables\":[\"db.fact\"],"
                        + "\"dimensions\":[\"dt\"],\"measures\":[\"sum\"],\"refreshTime\":2000,"
                        + "\"permissionGranted\":false}]");

        DorisMvRewriteTransformer transformer = new DorisMvRewriteTransformer();
        String originSql = "select dt, sum(price) from db.fact group by dt";
        try (KylinConfig.SetAndUnsetThreadLocalConfig ignored = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            String rewritten = transformer.transform(originSql, "", "default");
            Assertions.assertEquals(originSql, rewritten);
        }
    }

    private String trimAndCompactWhitespaces(String sql) {
        return sql.replaceAll("\\s+", " ").trim();
    }
}
