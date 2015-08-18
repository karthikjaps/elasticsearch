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

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.CustomNumericDocValuesField;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;

import java.io.IOException;
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
public class GeoPointFieldMapperLegacy extends BaseGeoPointFieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "geo_point";

    public static class Names extends BaseGeoPointFieldMapper.Names {
        public static final String COERCE = "coerce";
    }

    public static class Defaults extends BaseGeoPointFieldMapper.Defaults{
        public static final boolean COERCE = false;

        public static final BaseGeoPointFieldType FIELD_TYPE = new GeoPointFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    /**
     * Concrete builder for legacy GeoPointField
     */
    public static class Builder extends BaseGeoPointFieldMapper.Builder<Builder, GeoPointFieldMapperLegacy> {

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
        public GeoPointFieldMapperLegacy build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                               MappedFieldType defaultFieldType, Settings indexSettings, ContentPath.Type pathType, DoubleFieldMapper latMapper,
                                               DoubleFieldMapper lonMapper, StringFieldMapper geohashMapper, MultiFields multiFields, CopyTo copyTo) {
            fieldType.setTokenized(false);
            setupFieldType(context);
            fieldType.setHasDocValues(false);
            defaultFieldType.setHasDocValues(false);
            return new GeoPointFieldMapperLegacy(simpleName, fieldType, defaultFieldType, indexSettings, pathType, latMapper, lonMapper,
                    geohashMapper, multiFields, copyTo);
        }

        @Override
        public GeoPointFieldMapperLegacy build(BuilderContext context) {
            return super.build(context);
        }
    }

    public static Builder parse(Builder builder, Map<String, Object> node, Mapper.TypeParser.ParserContext parserContext) throws MapperParsingException {
        final boolean indexCreatedBeforeV2_0 = parserContext.indexVersionCreated().before(Version.V_2_0_0);
        for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals(Names.IGNORE_MALFORMED)) {
                if (builder.fieldType().coerce == false) {
                    builder.fieldType().ignoreMalformed = XContentMapValues.nodeBooleanValue(fieldNode);
                }
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && fieldName.equals("validate")) {
                if (builder.fieldType().ignoreMalformed == false) {
                    builder.fieldType().ignoreMalformed = !XContentMapValues.nodeBooleanValue(fieldNode);
                }
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && fieldName.equals("validate_lon")) {
                if (builder.fieldType().ignoreMalformed() == false) {
                    builder.fieldType().ignoreMalformed = !XContentMapValues.nodeBooleanValue(fieldNode);
                }
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && fieldName.equals("validate_lat")) {
                if (builder.fieldType().ignoreMalformed == false) {
                    builder.fieldType().ignoreMalformed = !XContentMapValues.nodeBooleanValue(fieldNode);
                }
                iterator.remove();
            } else if (fieldName.equals(Names.COERCE)) {
                builder.fieldType().coerce = XContentMapValues.nodeBooleanValue(fieldNode);
                if (builder.fieldType().coerce == true) {
                    builder.fieldType().ignoreMalformed = true;
                }
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && fieldName.equals("normalize")) {
                builder.fieldType().coerce = XContentMapValues.nodeBooleanValue(fieldNode);
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && fieldName.equals("normalize_lat")) {
                builder.fieldType().coerce = XContentMapValues.nodeBooleanValue(fieldNode);
                iterator.remove();
            } else if (indexCreatedBeforeV2_0 && fieldName.equals("normalize_lon")) {
                if (builder.fieldType().coerce == false) {
                    builder.fieldType().coerce = XContentMapValues.nodeBooleanValue(fieldNode);
                }
                iterator.remove();
            }
        }
        return builder;
    }

    public static final class GeoPointFieldType extends GeoPointFieldMapper.BaseGeoPointFieldType {
        protected boolean coerce = false;

        public GeoPointFieldType() {
            super();
        }

        protected GeoPointFieldType(GeoPointFieldType ref) {
            super(ref);
            this.coerce = ref.coerce;
        }

        @Override
        public MappedFieldType clone() {
            return new GeoPointFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            GeoPointFieldType that = (GeoPointFieldType) o;
            return coerce == that.coerce;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(super.hashCode(), coerce);
        }

        public boolean coerce() {
            return this.coerce;
        }

        public void setCoerce(boolean coerce) {
            checkIfFrozen();
            this.coerce = coerce;
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

    public GeoPointFieldMapperLegacy(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings,
                                     ContentPath.Type pathType, DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper,
                                     StringFieldMapper geohashMapper, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, pathType, latMapper, lonMapper, geohashMapper, multiFields, copyTo);
    }

    @Override
    public GeoPointFieldType fieldType() {
        return (GeoPointFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    protected void parse(ParseContext context, GeoPoint point, String geohash) throws IOException {
        if (fieldType().ignoreMalformed == false) {
            if (point.lat() > 90.0 || point.lat() < -90.0) {
                throw new IllegalArgumentException("illegal latitude value [" + point.lat() + "] for " + name());
            }
            if (point.lon() > 180.0 || point.lon() < -180) {
                throw new IllegalArgumentException("illegal longitude value [" + point.lon() + "] for " + name());
            }
        }

        if (fieldType().coerce) {
            // by setting coerce to false we are assuming all geopoints are already in a valid coordinate system
            // thus this extra step can be skipped
            GeoUtils.normalizePoint(point, true, true);
        }

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().names().indexName(), Double.toString(point.lat()) + ',' + Double.toString(point.lon()), fieldType());
            context.doc().add(field);
        }

        super.parse(context, point, geohash);

        if (fieldType().hasDocValues()) {
            CustomGeoPointDocValuesField field = (CustomGeoPointDocValuesField) context.doc().getByKey(fieldType().names().indexName());
            if (field == null) {
                field = new CustomGeoPointDocValuesField(fieldType().names().indexName(), point.lat(), point.lon());
                context.doc().addWithKey(fieldType().names().indexName(), field);
            } else {
                field.add(point.lat(), point.lon());
            }
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || fieldType().coerce != Defaults.COERCE) {
            builder.field(Names.COERCE, fieldType().coerce);
        }
    }

    public static class CustomGeoPointDocValuesField extends CustomNumericDocValuesField {

        private final ObjectHashSet<GeoPoint> points;

        public CustomGeoPointDocValuesField(String name, double lat, double lon) {
            super(name);
            points = new ObjectHashSet<>(2);
            points.add(new GeoPoint(lat, lon));
        }

        public void add(double lat, double lon) {
            points.add(new GeoPoint(lat, lon));
        }

        @Override
        public BytesRef binaryValue() {
            final byte[] bytes = new byte[points.size() * 16];
            int off = 0;
            for (Iterator<ObjectCursor<GeoPoint>> it = points.iterator(); it.hasNext(); ) {
                final GeoPoint point = it.next().value;
                ByteUtils.writeDoubleLE(point.getLat(), bytes, off);
                ByteUtils.writeDoubleLE(point.getLon(), bytes, off + 8);
                off += 16;
            }
            return new BytesRef(bytes);
        }
    }

}