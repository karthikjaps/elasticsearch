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

package org.elasticsearch.index.mapper.geo;

import com.google.common.collect.Iterators;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.document.GeoPointField;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Parsing: We handle:
 * <p/>
 * - "field" : "geo_hash"
 * - "field" : "lat,lon"
 * - "field" : {
 * "lat" : 1.1,
 * "lon" : 2.1
 * }
 */
public class GeoPointFieldMapper extends BaseGeoPointFieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "geo_point";

    public static class Names extends BaseGeoPointFieldMapper.Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults extends BaseGeoPointFieldMapper.Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);

        public static final BaseGeoPointFieldType FIELD_TYPE = new GeoPointFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setNumericType(FieldType.NumericType.LONG);
            FIELD_TYPE.setNumericPrecisionStep(GeoPointField.PRECISION_STEP);
            FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.freeze();
        }
    }

    /**
     * Concrete builder for indexed GeoPointField type
     */
    public static class Builder extends BaseGeoPointFieldMapper.Builder<Builder, GeoPointFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        @Override
        public GeoPointFieldType fieldType() {
            return (GeoPointFieldType)fieldType;
        }

        @Override
        public Builder multiFieldPathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        @Override
        public Builder fieldDataSettings(Settings settings) {
            this.fieldDataSettings = settings;
            return builder;
        }

        @Override
        public GeoPointFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
              MappedFieldType defaultFieldType, Settings indexSettings, ContentPath.Type pathType, DoubleFieldMapper latMapper,
              DoubleFieldMapper lonMapper, StringFieldMapper geohashMapper, MultiFields multiFields, CopyTo copyTo) {
            fieldType.setTokenized(false);
            setupFieldType(context);
            return new GeoPointFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, pathType, latMapper, lonMapper,
                    geohashMapper, multiFields, copyTo);
        }

        @Override
        public GeoPointFieldMapper build(BuilderContext context) {
            return super.build(context);
        }
    }

    protected static Builder parse(Builder builder, Map<String, Object> node) throws MapperParsingException {
        for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals(Names.IGNORE_MALFORMED)) {
                builder.fieldType().ignoreMalformed = XContentMapValues.nodeBooleanValue(fieldNode);
                iterator.remove();
            }
        }
        return builder;
    }

    public static class TypeParser extends BaseGeoPointFieldMapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return super.parse(name, node, parserContext);
        }
    }

    public static final class GeoPointFieldType extends BaseGeoPointFieldType {
        private boolean ignoreMalformed = false;

        public GeoPointFieldType() {
        }

        protected GeoPointFieldType(GeoPointFieldType ref) {
            super(ref);
            this.ignoreMalformed = ref.ignoreMalformed;
        }

        @Override
        public MappedFieldType clone() {
            return new GeoPointFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            GeoPointFieldType that = (GeoPointFieldType) o;
            return ignoreMalformed == that.ignoreMalformed;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(super.hashCode(), ignoreMalformed);
        }

        public boolean ignoreMalformed() {
            return ignoreMalformed;
        }

        public void setIgnoreMalformed(boolean ignoreMalformed) {
            checkIfFrozen();
            this.ignoreMalformed = ignoreMalformed;
        }

        @Override
        public GeoPoint value(Object value) {
            if (value instanceof GeoPoint) {
                return (GeoPoint) value;
            } else {
                return GeoPoint.parseFromLatLon(value.toString());
            }
        }
    }

    public GeoPointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings,
                               ContentPath.Type pathType, DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper,
                               StringFieldMapper geohashMapper, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, pathType, latMapper, lonMapper, geohashMapper, multiFields, copyTo);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public GeoPointFieldType fieldType() {
        return (GeoPointFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    protected void parse(ParseContext context, GeoPoint point, String geohash) throws IOException {
        if (!fieldType().ignoreMalformed) {
            if (point.lat() > 90.0 || point.lat() < -90.0) {
                throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
            }

            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
            }
        }

        // LUCENE WATCH: This will be folded back into Lucene's GeoPointField
        GeoUtils.normalizePoint(point);

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            context.doc().add(new GeoPointField(fieldType().names().indexName(), point.lon(), point.lat(), fieldType() ));
        }

        super.parse(context, point, geohash);
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> extras = new ArrayList<>();
        if (fieldType().isGeohashEnabled()) {
            extras.add(geohashMapper);
        }
        return Iterators.concat(super.iterator(), extras.iterator());
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || fieldType().ignoreMalformed != Defaults.IGNORE_MALFORMED.value()) {
            builder.field(Names.IGNORE_MALFORMED, fieldType().ignoreMalformed);
        }
    }
}
