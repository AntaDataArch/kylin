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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * MVP transformer for replacing a fact table with a configured materialized view candidate.
 * The rewrite supports a Doris SQL subset and always falls back to the original SQL when unsafe.
 */
public class DorisMvRewriteTransformer implements IQueryTransformer, IPushDownConverter {

    private static final Logger logger = LoggerFactory.getLogger(DorisMvRewriteTransformer.class);
    private static final TypeReference<List<DorisMvMetadata>> METADATA_LIST_TYPE = new TypeReference<List<DorisMvMetadata>>() {
    };

    private static final Pattern FROM_PATTERN = Pattern.compile("(?is)\\bfrom\\s+([`\"\\w.]+)");
    private static final Pattern JOIN_TABLE_PATTERN = Pattern.compile("(?is)\\bjoin\\s+([`\"\\w.]+)");
    private static final Pattern JOIN_CONDITION_PATTERN = Pattern
            .compile("(?is)\\bjoin\\s+[`\"\\w.]+(?:\\s+\\w+)?\\s+on\\s+(.+?)(?=\\bjoin\\b|\\bwhere\\b|\\bgroup\\b|\\border\\b|\\blimit\\b|$)");
    private static final Pattern GROUP_BY_PATTERN = Pattern.compile("(?is)\\bgroup\\s+by\\s+(.+?)(?=\\border\\b|\\blimit\\b|\\bhaving\\b|$)");
    private static final Pattern COUNT_DISTINCT_PATTERN = Pattern.compile("(?is)\\bcount\\s*\\(\\s*distinct\\b");
    private static final Pattern COUNT_PATTERN = Pattern.compile("(?is)\\bcount\\s*\\(");
    private static final Pattern SUM_PATTERN = Pattern.compile("(?is)\\bsum\\s*\\(");
    private static final Pattern WINDOW_FUNCTION_PATTERN = Pattern.compile("(?is)\\bover\\s*\\(");
    // Candidate ranking constants: unknown row count should be heavily penalized,
    // then additional penalties for join complexity and group-by complexity.
    private static final double JOIN_PENALTY_WEIGHT = 1_000_000D;
    private static final double GROUP_BY_PENALTY_WEIGHT = 100_000D;
    // Keep large enough to discourage unknown-row-count candidates while avoiding overflow in subsequent math.
    private static final long UNKNOWN_ROW_COUNT_SUBSTITUTE = Long.MAX_VALUE / 4;

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        return rewrite(sql, project);
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        return rewrite(originSql, project);
    }

    private String rewrite(String sql, String project) {
        if (StringUtils.isBlank(sql)) {
            return sql;
        }
        KylinConfig kylinConfig = getProjectConfig(project);
        if (!kylinConfig.isDorisMvRewriteEnabled()) {
            return sql;
        }
        if (!isSupportedSubset(sql)) {
            return sql;
        }
        Optional<DorisQueryPattern> queryPattern = parseQueryPattern(sql);
        if (!queryPattern.isPresent()) {
            return sql;
        }

        List<DorisMvMetadata> candidates = parseMetadata(kylinConfig.getDorisMvRewriteMetadataJson());
        if (CollectionUtils.isEmpty(candidates)) {
            return sql;
        }

        Optional<DorisMvMetadata> selected = selectCandidate(queryPattern.get(), candidates, kylinConfig);
        if (!selected.isPresent()) {
            return sql;
        }
        DorisMvMetadata best = selected.get();
        if (!isSafeToRewrite(best)) {
            return sql;
        }
        return replaceFactTable(sql, queryPattern.get().factTable, best.mvTable);
    }

    private boolean isSupportedSubset(String sql) {
        String normalized = StringUtils.trim(sql).toLowerCase(Locale.ROOT);
        return normalized.startsWith("select") && !normalized.contains(" union ")
                && !WINDOW_FUNCTION_PATTERN.matcher(normalized).find()
                && !normalized.contains(" with ");
    }

    private Optional<DorisQueryPattern> parseQueryPattern(String sql) {
        Matcher fromMatcher = FROM_PATTERN.matcher(sql);
        if (!fromMatcher.find()) {
            return Optional.empty();
        }
        DorisQueryPattern pattern = new DorisQueryPattern();
        pattern.factTable = normalizeIdentifier(fromMatcher.group(1));
        pattern.tables.add(pattern.factTable);

        Matcher joinTableMatcher = JOIN_TABLE_PATTERN.matcher(sql);
        while (joinTableMatcher.find()) {
            pattern.tables.add(normalizeIdentifier(joinTableMatcher.group(1)));
        }

        Matcher joinConditionMatcher = JOIN_CONDITION_PATTERN.matcher(sql);
        while (joinConditionMatcher.find()) {
            pattern.joinSignatures.add(normalizeExpression(joinConditionMatcher.group(1)));
        }

        Matcher groupByMatcher = GROUP_BY_PATTERN.matcher(sql);
        if (groupByMatcher.find()) {
            String groupExpr = groupByMatcher.group(1);
            for (String part : groupExpr.split(",")) {
                String dim = normalizeExpression(part);
                if (StringUtils.isNotBlank(dim)) {
                    pattern.groupByColumns.add(dim);
                }
            }
        }

        String lowered = sql.toLowerCase(Locale.ROOT);
        if (COUNT_DISTINCT_PATTERN.matcher(lowered).find()) {
            pattern.measures.add("count_distinct");
        }
        if (COUNT_PATTERN.matcher(lowered).find()) {
            pattern.measures.add("count");
        }
        if (SUM_PATTERN.matcher(lowered).find()) {
            pattern.measures.add("sum");
        }

        return Optional.of(pattern);
    }

    private List<DorisMvMetadata> parseMetadata(String metadataJson) {
        if (StringUtils.isBlank(metadataJson)) {
            return Collections.emptyList();
        }
        try {
            return JsonUtil.readValue(metadataJson, METADATA_LIST_TYPE);
        } catch (IOException ex) {
            logger.warn("Failed to parse Doris MV rewrite metadata, skip rewrite.", ex);
            return Collections.emptyList();
        }
    }

    private Optional<DorisMvMetadata> selectCandidate(DorisQueryPattern pattern, List<DorisMvMetadata> candidates,
            KylinConfig config) {
        final long now = System.currentTimeMillis();
        return candidates.stream().filter(candidate -> matches(pattern, candidate))
                .filter(candidate -> checkFreshness(candidate, now, config.getDorisMvRewriteMaxStalenessSeconds()))
                .sorted(Comparator.comparingLong((DorisMvMetadata c) -> c.refreshTime).reversed()
                        .thenComparingInt((DorisMvMetadata c) -> safeSize(c.coveredTables)).reversed()
                        .thenComparingDouble(c -> estimateCost(c, pattern)))
                .findFirst();
    }

    private boolean matches(DorisQueryPattern pattern, DorisMvMetadata candidate) {
        if (StringUtils.isBlank(candidate.mvTable) || StringUtils.isBlank(candidate.factTable)) {
            return false;
        }
        if (!normalizeIdentifier(candidate.factTable).equals(pattern.factTable)) {
            return false;
        }
        Set<String> normalizedCoveredTables = normalizeSet(candidate.coveredTables, this::normalizeIdentifier);
        if (!normalizedCoveredTables.containsAll(pattern.tables)) {
            return false;
        }
        Set<String> normalizedJoinSignatures = normalizeSet(candidate.joinSignatures, this::normalizeExpression);
        // Empty candidate join signatures mean "no strict join-signature constraint"
        // (intentional for denormalized or pre-joined MVs).
        if (!normalizedJoinSignatures.isEmpty() && !normalizedJoinSignatures.containsAll(pattern.joinSignatures)) {
            return false;
        }
        Set<String> normalizedDimensions = normalizeSet(candidate.dimensions, this::normalizeExpression);
        if (!normalizedDimensions.containsAll(pattern.groupByColumns)) {
            return false;
        }
        Set<String> normalizedMeasures = normalizeSet(candidate.measures, s -> StringUtils.lowerCase(StringUtils.trim(s)));
        return normalizedMeasures.containsAll(pattern.measures);
    }

    private boolean checkFreshness(DorisMvMetadata candidate, long now, long maxStalenessSeconds) {
        if (maxStalenessSeconds < 0) {
            return true;
        }
        if (candidate.refreshTime <= 0L) {
            return false;
        }
        long maxStalenessMillis = maxStalenessSeconds > Long.MAX_VALUE / 1000 ? Long.MAX_VALUE
                : maxStalenessSeconds * 1000L;
        return now - candidate.refreshTime <= maxStalenessMillis;
    }

    private boolean isSafeToRewrite(DorisMvMetadata candidate) {
        return candidate.available && candidate.permissionGranted && candidate.partitionAvailable
                && candidate.columnTypeCompatible;
    }

    private double estimateCost(DorisMvMetadata candidate, DorisQueryPattern pattern) {
        long rowCost = candidate.rowCount <= 0 ? UNKNOWN_ROW_COUNT_SUBSTITUTE : candidate.rowCount;
        double pruningPenalty = Math.max(0D, 1D - candidate.partitionPruningRatio) * rowCost;
        double joinPenalty = Math.max(0, safeSize(candidate.joinSignatures) - pattern.joinSignatures.size())
                * JOIN_PENALTY_WEIGHT;
        double groupPenalty = Math.max(0, safeSize(candidate.dimensions) - pattern.groupByColumns.size())
                * GROUP_BY_PENALTY_WEIGHT;
        return rowCost + pruningPenalty + joinPenalty + groupPenalty;
    }

    private String replaceFactTable(String sql, String factTable, String mvTable) {
        Matcher matcher = FROM_PATTERN.matcher(sql);
        if (!matcher.find()) {
            return sql;
        }
        if (!normalizeIdentifier(matcher.group(1)).equals(factTable)) {
            return sql;
        }
        String replacement = formatIdentifierLikeSource(matcher.group(1), mvTable);
        return sql.substring(0, matcher.start(1)) + replacement + sql.substring(matcher.end(1));
    }

    private String formatIdentifierLikeSource(String sourceIdentifier, String targetIdentifier) {
        if (StringUtils.startsWith(sourceIdentifier, "`")) {
            return quoteIdentifier(targetIdentifier, "`");
        }
        if (StringUtils.startsWith(sourceIdentifier, "\"")) {
            return quoteIdentifier(targetIdentifier, "\"");
        }
        return targetIdentifier;
    }

    private String quoteIdentifier(String identifier, String quote) {
        return Arrays.stream(normalizeIdentifier(identifier).split("\\.")).map(part -> quote + part + quote)
                .collect(Collectors.joining("."));
    }

    private Set<String> normalizeSet(Set<String> source, java.util.function.Function<String, String> normalizer) {
        if (source == null) {
            return Collections.emptySet();
        }
        return source.stream().map(normalizer).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
    }

    private String normalizeIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        String normalized = StringUtils.remove(identifier, "`");
        normalized = StringUtils.remove(normalized, "\"");
        return StringUtils.lowerCase(StringUtils.trim(normalized), Locale.ROOT);
    }

    private String normalizeExpression(String expression) {
        if (expression == null) {
            return null;
        }
        String normalized = normalizeIdentifier(expression);
        return normalized == null ? null : normalized.replaceAll("\\s+", " ");
    }

    private KylinConfig getProjectConfig(String project) {
        if (StringUtils.isBlank(project)) {
            return KylinConfig.getInstanceFromEnv();
        }
        try {
            return NProjectManager.getProjectConfig(project);
        } catch (Exception ex) {
            logger.debug("Use env config for Doris MV rewrite because project config is unavailable: {}", project);
            return KylinConfig.getInstanceFromEnv();
        }
    }

    private int safeSize(Set<String> set) {
        return set == null ? 0 : set.size();
    }

    static class DorisQueryPattern {
        private String factTable;
        private final Set<String> tables = new LinkedHashSet<>();
        private final Set<String> joinSignatures = new LinkedHashSet<>();
        private final Set<String> groupByColumns = new LinkedHashSet<>();
        private final Set<String> measures = new LinkedHashSet<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DorisMvMetadata {
        public String mvTable;
        public String factTable;
        public Set<String> coveredTables = new LinkedHashSet<>();
        public Set<String> joinSignatures = new LinkedHashSet<>();
        public Set<String> dimensions = new LinkedHashSet<>();
        public Set<String> measures = new LinkedHashSet<>();
        public String partitionKey;
        public long refreshTime = 0L;
        public boolean available = true;
        public boolean permissionGranted = true;
        public boolean partitionAvailable = true;
        public boolean columnTypeCompatible = true;
        public long rowCount = -1L;
        public double partitionPruningRatio = 1.0D;
    }
}
