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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.GeoHashUtils;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.doubleField;
import static org.elasticsearch.index.mapper.MapperBuilders.geoPointField;
import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;

/**
 *
 */
public abstract class BaseGeoPointFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "geo_point";

    public static class Names {
        public static final String LAT = "lat";
        public static final String LAT_SUFFIX = "." + LAT;
        public static final String LON = "lon";
        public static final String LON_SUFFIX = "." + LON;
        public static final String GEOHASH = "geohash";
        public static final String GEOHASH_SUFFIX = "." + GEOHASH;
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
        public static final boolean ENABLE_LATLON = false;
        public static final boolean ENABLE_GEOHASH = false;
        public static final boolean ENABLE_GEOHASH_PREFIX = false;
        public static final int GEO_HASH_PRECISION = GeoHashUtils.PRECISION;
        public static final boolean IGNORE_MALFORMED = false;
    }

    public abstract static class Builder<T extends Builder, Y extends BaseGeoPointFieldMapper> extends FieldMapper.Builder<T, Y> {
        protected ContentPath.Type pathType = Defaults.PATH_TYPE;

        protected boolean enableLatLon = Defaults.ENABLE_LATLON;

        protected Integer precisionStep;

        protected boolean enableGeoHash = Defaults.ENABLE_GEOHASH;

        protected boolean enableGeohashPrefix = Defaults.ENABLE_GEOHASH_PREFIX;

        protected int geoHashPrecision = Defaults.GEO_HASH_PRECISION;

        public Builder(String name, BaseGeoPointFieldType fieldType) {
            super(name, fieldType);
        }

        public T enableLatLon(boolean enableLatLon) {
            this.enableLatLon = enableLatLon;
            return builder;
        }

        public T precisionStep(int precisionStep) {
            this.precisionStep = precisionStep;
            return builder;
        }

        public T enableGeoHash(boolean enableGeoHash) {
            this.enableGeoHash = enableGeoHash;
            return builder;
        }

        public T geohashPrefix(boolean enableGeohashPrefix) {
            this.enableGeohashPrefix = enableGeohashPrefix;
            return builder;
        }

        public T geoHashPrecision(int precision) {
            this.geoHashPrecision = precision;
            return builder;
        }

        public abstract Y build(BuilderContext context, String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, ContentPath.Type pathType, DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper,
                                StringFieldMapper geohashMapper, MultiFields multiFields, CopyTo copyTo);

        public Y build(Mapper.BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            BaseGeoPointFieldType geoPointFieldType = (BaseGeoPointFieldType)fieldType;

            DoubleFieldMapper latMapper = null;
            DoubleFieldMapper lonMapper = null;

            context.path().add(name);
            if (enableLatLon) {
                NumberFieldMapper.Builder<?, ?> latMapperBuilder = doubleField(Names.LAT).includeInAll(false);
                NumberFieldMapper.Builder<?, ?> lonMapperBuilder = doubleField(Names.LON).includeInAll(false);
                if (precisionStep != null) {
                    latMapperBuilder.precisionStep(precisionStep);
                    lonMapperBuilder.precisionStep(precisionStep);
                }
                latMapper = (DoubleFieldMapper) latMapperBuilder.includeInAll(false).store(fieldType.stored()).docValues(false).build(context);
                lonMapper = (DoubleFieldMapper) lonMapperBuilder.includeInAll(false).store(fieldType.stored()).docValues(false).build(context);
                geoPointFieldType.setLatLonEnabled(latMapper.fieldType(), lonMapper.fieldType());
            }
            StringFieldMapper geohashMapper = null;
            if (enableGeoHash || enableGeohashPrefix) {
                // TODO: possible also implicitly enable geohash if geohash precision is set
                geohashMapper = stringField(Names.GEOHASH).index(true).tokenized(false).includeInAll(false).omitNorms(true).indexOptions(IndexOptions.DOCS).build(context);
                geoPointFieldType.setGeohashEnabled(geohashMapper.fieldType(), geoHashPrecision, enableGeohashPrefix);
            }
            context.path().remove();
            context.path().pathType(origPathType);

            return build(context, name, fieldType, defaultFieldType, context.indexSettings(), origPathType,
                    latMapper, lonMapper, geohashMapper, multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public abstract static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = geoPointField(name, parserContext.indexVersionCreated());
            parseField(builder, name, node, parserContext);

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("lat_lon")) {
                    builder.enableLatLon(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("precision_step")) {
                    builder.precisionStep(XContentMapValues.nodeIntegerValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("path") && parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {
                    builder.multiFieldPathType(parsePathType(name, fieldNode.toString()));
                    iterator.remove();
                } else if (fieldName.equals("geohash")) {
                    builder.enableGeoHash(XContentMapValues.nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("geohash_prefix")) {
                    builder.geohashPrefix(XContentMapValues.nodeBooleanValue(fieldNode));
                    if (XContentMapValues.nodeBooleanValue(fieldNode)) {
                        builder.enableGeoHash(true);
                    }
                    iterator.remove();
                } else if (fieldName.equals("geohash_precision")) {
                    if (fieldNode instanceof Integer) {
                        builder.geoHashPrecision(XContentMapValues.nodeIntegerValue(fieldNode));
                    } else {
                        builder.geoHashPrecision(GeoUtils.geoHashLevelsForPrecision(fieldNode.toString()));
                    }
                    iterator.remove();
                } else if (parseMultiField(builder, name, parserContext, fieldName, fieldNode)) {
                    iterator.remove();
                }
            }

            if (builder instanceof GeoPointFieldMapperLegacy.Builder) {
                return GeoPointFieldMapperLegacy.parse((GeoPointFieldMapperLegacy.Builder) builder, node, parserContext);
            }

            return GeoPointFieldMapper.parse((GeoPointFieldMapper.Builder) builder, node);
        }
    }

    public abstract static class BaseGeoPointFieldType extends MappedFieldType {
        protected MappedFieldType geohashFieldType;
        protected int geohashPrecision;
        protected boolean geohashPrefixEnabled;

        protected MappedFieldType latFieldType;
        protected MappedFieldType lonFieldType;

        protected boolean ignoreMalformed = false;

        BaseGeoPointFieldType() {}

        BaseGeoPointFieldType(BaseGeoPointFieldType ref) {
            super(ref);
            this.geohashFieldType = ref.geohashFieldType; // copying ref is ok, this can never be modified
            this.geohashPrecision = ref.geohashPrecision;
            this.geohashPrefixEnabled = ref.geohashPrefixEnabled;
            this.latFieldType = ref.latFieldType; // copying ref is ok, this can never be modified
            this.lonFieldType = ref.lonFieldType; // copying ref is ok, this can never be modified
            this.ignoreMalformed = ref.ignoreMalformed;
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) return false;
            BaseGeoPointFieldType that = (BaseGeoPointFieldType) o;
            return  geohashPrecision == that.geohashPrecision &&
                    geohashPrefixEnabled == that.geohashPrefixEnabled &&
                    java.util.Objects.equals(geohashFieldType, that.geohashFieldType) &&
                    java.util.Objects.equals(latFieldType, that.latFieldType) &&
                    java.util.Objects.equals(lonFieldType, that.lonFieldType) &&
                    ignoreMalformed == that.ignoreMalformed;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(super.hashCode(), geohashFieldType, geohashPrecision, geohashPrefixEnabled, latFieldType,
                    lonFieldType, ignoreMalformed);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            BaseGeoPointFieldType other = (BaseGeoPointFieldType)fieldType;
            if (isLatLonEnabled() != other.isLatLonEnabled()) {
                conflicts.add("mapper [" + names().fullName() + "] has different lat_lon");
            }
            if (isLatLonEnabled() && other.isLatLonEnabled() &&
                    latFieldType().numericPrecisionStep() != other.latFieldType().numericPrecisionStep()) {
                conflicts.add("mapper [" + names().fullName() + "] has different precision_step");
            }
            if (isGeohashEnabled() != other.isGeohashEnabled()) {
                conflicts.add("mapper [" + names().fullName() + "] has different geohash");
            }
            if (geohashPrecision() != other.geohashPrecision()) {
                conflicts.add("mapper [" + names().fullName() + "] has different geohash_precision");
            }
            if (isGeohashPrefixEnabled() != other.isGeohashPrefixEnabled()) {
                conflicts.add("mapper [" + names().fullName() + "] has different geohash_prefix");
            }
        }

        public boolean isGeohashEnabled() {
            return geohashFieldType != null;
        }

        public MappedFieldType geohashFieldType() {
            return geohashFieldType;
        }

        public int geohashPrecision() {
            return geohashPrecision;
        }

        public boolean isGeohashPrefixEnabled() {
            return geohashPrefixEnabled;
        }

        public void setGeohashEnabled(MappedFieldType geohashFieldType, int geohashPrecision, boolean geohashPrefixEnabled) {
            checkIfFrozen();
            this.geohashFieldType = geohashFieldType;
            this.geohashPrecision = geohashPrecision;
            this.geohashPrefixEnabled = geohashPrefixEnabled;
        }

        public boolean isLatLonEnabled() {
            return latFieldType != null;
        }

        public MappedFieldType latFieldType() {
            return latFieldType;
        }

        public MappedFieldType lonFieldType() {
            return lonFieldType;
        }

        public void setLatLonEnabled(MappedFieldType latFieldType, MappedFieldType lonFieldType) {
            checkIfFrozen();
            this.latFieldType = latFieldType;
            this.lonFieldType = lonFieldType;
        }

        public boolean ignoreMalformed() {
            return ignoreMalformed;
        }

        public void setIgnoreMalformed(boolean ignoreMalformed) {
            checkIfFrozen();
            this.ignoreMalformed = ignoreMalformed;
        }
    }

    protected final DoubleFieldMapper latMapper;

    protected final DoubleFieldMapper lonMapper;

    protected final ContentPath.Type pathType;

    protected final StringFieldMapper geohashMapper;


    protected BaseGeoPointFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings,
           ContentPath.Type pathType, DoubleFieldMapper latMapper, DoubleFieldMapper lonMapper, StringFieldMapper geohashMapper,
           MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.pathType = pathType;
        this.latMapper = latMapper;
        this.lonMapper = lonMapper;
        this.geohashMapper = geohashMapper;
    }

    @Override
    public BaseGeoPointFieldType fieldType() {
        return (BaseGeoPointFieldType) super.fieldType();
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> extras = new ArrayList<>();
        if (fieldType().isGeohashEnabled()) {
            extras.add(geohashMapper);
        }
        if (fieldType().isLatLonEnabled()) {
            extras.add(latMapper);
            extras.add(lonMapper);
        }
        return Iterators.concat(super.iterator(), extras.iterator());
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    protected void parse(ParseContext context, GeoPoint point, String geohash) throws IOException {
        if (fieldType().isGeohashEnabled()) {
            if (geohash == null) {
                geohash = GeoHashUtils.stringEncode(point.lon(), point.lat());
            }
            addGeohashField(context, geohash);
        }
        if (fieldType().isLatLonEnabled()) {
            latMapper.parse(context.createExternalValueContext(point.lat()));
            lonMapper.parse(context.createExternalValueContext(point.lon()));
        }
        multiFields.parse(this, context);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);
        context.path().add(simpleName());

        GeoPoint sparse = context.parseExternalValue(GeoPoint.class);

        if (sparse != null) {
            parse(context, sparse, null);
        } else {
            sparse = new GeoPoint();
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                token = context.parser().nextToken();
                if (token == XContentParser.Token.START_ARRAY) {
                    // its an array of array of lon/lat [ [1.2, 1.3], [1.4, 1.5] ]
                    while (token != XContentParser.Token.END_ARRAY) {
                        parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                        token = context.parser().nextToken();
                    }
                } else {
                    // its an array of other possible values
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        double lon = context.parser().doubleValue();
                        token = context.parser().nextToken();
                        double lat = context.parser().doubleValue();
                        while ((token = context.parser().nextToken()) != XContentParser.Token.END_ARRAY);
                        parse(context, sparse.reset(lat, lon), null);
                    } else {
                        while (token != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                parsePointFromString(context, sparse, context.parser().text());
                            } else {
                                parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
                            }
                            token = context.parser().nextToken();
                        }
                    }
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                parsePointFromString(context, sparse, context.parser().text());
            } else if (token != XContentParser.Token.VALUE_NULL) {
                parse(context, GeoUtils.parseGeoPoint(context.parser(), sparse), null);
            }
        }

        context.path().remove();
        context.path().pathType(origPathType);
        return null;
    }

    private void addGeohashField(ParseContext context, String geohash) throws IOException {
        int len = Math.min(fieldType().geohashPrecision(), geohash.length());
        int min = fieldType().isGeohashPrefixEnabled() ? 1 : geohash.length();

        for (int i = len; i >= min; i--) {
            // side effect of this call is adding the field
            geohashMapper.parse(context.createExternalValueContext(geohash.substring(0, i)));
        }
    }

    private void parsePointFromString(ParseContext context, GeoPoint sparse, String point) throws IOException {
        if (point.indexOf(',') < 0) {
            parse(context, sparse.resetFromGeohashString(point), point);
        } else {
            parse(context, sparse.resetFromString(point), null);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || pathType != Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase(Locale.ROOT));
        }
        if (includeDefaults || fieldType().isLatLonEnabled() != GeoPointFieldMapper.Defaults.ENABLE_LATLON) {
            builder.field("lat_lon", fieldType().isLatLonEnabled());
        }
        if (fieldType().isLatLonEnabled() && (includeDefaults || fieldType().latFieldType().numericPrecisionStep() != NumericUtils.PRECISION_STEP_DEFAULT)) {
            builder.field("precision_step", fieldType().latFieldType().numericPrecisionStep());
        }
        if (includeDefaults || fieldType().isGeohashEnabled() != Defaults.ENABLE_GEOHASH) {
            builder.field("geohash", fieldType().isGeohashEnabled());
        }
        if (includeDefaults || fieldType().isGeohashPrefixEnabled() != Defaults.ENABLE_GEOHASH_PREFIX) {
            builder.field("geohash_prefix", fieldType().isGeohashPrefixEnabled());
        }
        if (fieldType().isGeohashEnabled() && (includeDefaults || fieldType().geohashPrecision() != Defaults.GEO_HASH_PRECISION)) {
            builder.field("geohash_precision", fieldType().geohashPrecision());
        }
        if (includeDefaults || fieldType().ignoreMalformed != Defaults.IGNORE_MALFORMED) {
            builder.field(Names.IGNORE_MALFORMED, fieldType().ignoreMalformed);
        }
    }
}
