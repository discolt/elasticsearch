/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.vpack.search;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryShardContext;

import java.util.*;

/**
 * Helpers to extract and expand field names and boosts
 */
public final class QueryParserHelperExt {
    // Mapping types the "all-ish" query can be executed against
    // TODO: Fix the API so that we don't need a hardcoded list of types
    private static final Set<String> ALLOWED_QUERY_MAPPER_TYPES;

    static {
        ALLOWED_QUERY_MAPPER_TYPES = new HashSet<>();
        ALLOWED_QUERY_MAPPER_TYPES.add(DateFieldMapper.CONTENT_TYPE);
        ALLOWED_QUERY_MAPPER_TYPES.add(IpFieldMapper.CONTENT_TYPE);
        ALLOWED_QUERY_MAPPER_TYPES.add(KeywordFieldMapper.CONTENT_TYPE);
        for (NumberFieldMapper.NumberType nt : NumberFieldMapper.NumberType.values()) {
            ALLOWED_QUERY_MAPPER_TYPES.add(nt.typeName());
        }
        ALLOWED_QUERY_MAPPER_TYPES.add("scaled_float");
        ALLOWED_QUERY_MAPPER_TYPES.add(TextFieldMapper.CONTENT_TYPE);
    }

    private QueryParserHelperExt() {
    }

    /**
     * Convert a list of field names encoded with optional boosts to a map that associates
     * the field name and its boost.
     *
     * @param fields The list of fields encoded with optional boosts (e.g. ^0.35).
     * @return The converted map with field names and associated boosts.
     */
    public static Map<String, Map.Entry<Float, String>> parseFieldsAndWeights(List<String> fields) {
        final Map<String, Map.Entry<Float, String>> fieldsAndWeights = new HashMap<>();
        for (String field : fields) {
            int boostIndex = field.indexOf('^');
            String fieldName;
            float boost = 1.0f;
            if (boostIndex != -1) {
                fieldName = field.substring(0, boostIndex);
                boost = Float.parseFloat(field.substring(boostIndex + 1, field.length()));
            } else {
                fieldName = field;
            }
            fieldsAndWeights.put(fieldName, new AbstractMap.SimpleEntry<>(boost, null));
        }
        return fieldsAndWeights;
    }

    /**
     * Get a {@link FieldMapper} associated with a field name or null.
     *
     * @param mapperService The mapper service where to find the mapping.
     * @param field         The field name to search.
     */
    public static Mapper getFieldMapper(MapperService mapperService, String field) {
        for (DocumentMapper mapper : mapperService.docMappers(true)) {
            Mapper fieldMapper = mapper.mappers().getMapper(field);
            if (fieldMapper != null) {
                return fieldMapper;
            }
        }
        return null;
    }

    public static Map<String, Map.Entry<Float, String>> resolveMappingFields(QueryShardContext context,
                                                          Map<String, Map.Entry<Float, String>> fieldsAndWeights) {
        return resolveMappingFields(context, fieldsAndWeights, null);
    }

    /**
     * Resolve all the field names and patterns present in the provided map with the
     * {@link QueryShardContext} and returns a new map containing all the expanded fields with their original boost.
     *
     * @param context          The context of the query.
     * @param fieldsAndWeights The map of fields and weights to expand.
     * @param fieldSuffix      The suffix name to add to the expanded field names if a mapping exists for that name.
     *                         The original name of the field is kept if adding the suffix to the field name does not point to a valid field
     *                         in the mapping.
     */
    public static Map<String, Map.Entry<Float, String>> resolveMappingFields(QueryShardContext context,
                                                          Map<String, Map.Entry<Float, String>> fieldsAndWeights,
                                                          String fieldSuffix) {
        Map<String, Map.Entry<Float, String>> resolvedFields = new HashMap<>();
        for (Map.Entry<String, Map.Entry<Float, String>> fieldEntry : fieldsAndWeights.entrySet()) {
            boolean allField = Regex.isMatchAllPattern(fieldEntry.getKey());
            boolean multiField = Regex.isSimpleMatchPattern(fieldEntry.getKey());
            float weight = fieldEntry.getValue() == null ? 1.0f : fieldEntry.getValue().getKey();
            String sim = fieldEntry.getValue().getValue();
            Map<String, Map.Entry<Float, String>> fieldMap = resolveMappingField(context, fieldEntry.getKey(), weight, sim,
                    !multiField, !allField, fieldSuffix);
            resolvedFields.putAll(fieldMap);
        }
        return resolvedFields;
    }

    /**
     * Resolves the provided pattern or field name from the {@link QueryShardContext} and return a map of
     * the expanded fields with their original boost.
     *
     * @param context             The context of the query
     * @param fieldOrPattern      The field name or the pattern to resolve
     * @param weight              The weight for the field
     * @param acceptAllTypes      Whether all field type should be added when a pattern is expanded.
     *                            If false, only {@link #ALLOWED_QUERY_MAPPER_TYPES} are accepted and other field types
     *                            are discarded from the query.
     * @param acceptMetadataField Whether metadata fields should be added when a pattern is expanded.
     */
    public static Map<String, Map.Entry<Float, String>> resolveMappingField(QueryShardContext context, String fieldOrPattern, float weight, String sim,
                                                                            boolean acceptAllTypes, boolean acceptMetadataField) {
        return resolveMappingField(context, fieldOrPattern, weight, sim, acceptAllTypes, acceptMetadataField, null);
    }

    /**
     * Resolves the provided pattern or field name from the {@link QueryShardContext} and return a map of
     * the expanded fields with their original boost.
     *
     * @param context             The context of the query
     * @param fieldOrPattern      The field name or the pattern to resolve
     * @param weight              The weight for the field
     * @param acceptAllTypes      Whether all field type should be added when a pattern is expanded.
     *                            If false, only {@link #ALLOWED_QUERY_MAPPER_TYPES} are accepted and other field types
     *                            are discarded from the query.
     * @param acceptMetadataField Whether metadata fields should be added when a pattern is expanded.
     * @param fieldSuffix         The suffix name to add to the expanded field names if a mapping exists for that name.
     *                            The original name of the field is kept if adding the suffix to the field name does not point to a valid field
     *                            in the mapping.
     */
    public static Map<String, Map.Entry<Float, String>> resolveMappingField(QueryShardContext context, String fieldOrPattern, float weight, String sim,
                                                                            boolean acceptAllTypes, boolean acceptMetadataField, String fieldSuffix) {
        Collection<String> allFields = context.simpleMatchToIndexNames(fieldOrPattern);
        Map<String, Map.Entry<Float, String>> fields = new HashMap<>();
        for (String fieldName : allFields) {
            if (fieldSuffix != null && context.fieldMapper(fieldName + fieldSuffix) != null) {
                fieldName = fieldName + fieldSuffix;
            }

            MappedFieldType fieldType = context.getMapperService().fullName(fieldName);
            if (fieldType == null) {
                // Note that we don't ignore unmapped fields.
                fields.put(fieldName, new AbstractMap.SimpleEntry<>(weight, sim));
                continue;
            }

            // Ignore fields that are not in the allowed mapper types. Some
            // types do not support term queries, and thus we cannot generate
            // a special query for them.
            String mappingType = fieldType.typeName();
            if (acceptAllTypes == false && ALLOWED_QUERY_MAPPER_TYPES.contains(mappingType) == false) {
                continue;
            }

            // Ignore metadata fields.
            Mapper mapper = getFieldMapper(context.getMapperService(), fieldName);
            if (acceptMetadataField == false && mapper instanceof MetadataFieldMapper) {
                continue;
            }
            fields.put(fieldName, new AbstractMap.SimpleEntry<>(weight, sim));
        }
        return fields;
    }
}
