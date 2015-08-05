/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.geo;


import org.apache.lucene.util.GeoUtils;

import java.util.ArrayList;
import java.util.Collection;


/**
 * Utilities for encoding and decoding geohashes. Based on
 * http://en.wikipedia.org/wiki/Geohash.
 */
// LUCENE MONITOR: monitor against spatial package
// replaced with native DECODE_MAP
public class GeoHashUtils {

    private GeoHashUtils() {
    }

    private static final char encode(int x, int y) {
        return org.apache.lucene.util.GeoHashUtils.BASE_32[((x & 1) + ((y & 1) * 2) + ((x & 2) * 2) + ((y & 2) * 4) + ((x & 4) * 4)) % 32];
    }

    /**
     * Calculate all neighbors of a given geohash cell.
     *
     * @param geohash Geohash of the defined cell
     * @return geohashes of all neighbor cells
     */
    public static Collection<? extends CharSequence> neighbors(String geohash) {
        return addNeighbors(geohash, geohash.length(), new ArrayList<CharSequence>(8));
    }
    
    /**
     * Calculate the geohash of a neighbor of a geohash
     *
     * @param geohash the geohash of a cell
     * @param level   level of the geohash
     * @param dx      delta of the first grid coordinate (must be -1, 0 or +1)
     * @param dy      delta of the second grid coordinate (must be -1, 0 or +1)
     * @return geohash of the defined cell
     */
    private final static String neighbor(String geohash, int level, int dx, int dy) {
        int cell = org.apache.lucene.util.GeoHashUtils.BASE_32_STRING.indexOf(geohash.charAt(level -1));

        // Decoding the Geohash bit pattern to determine grid coordinates
        int x0 = cell & 1;  // first bit of x
        int y0 = cell & 2;  // first bit of y
        int x1 = cell & 4;  // second bit of x
        int y1 = cell & 8;  // second bit of y
        int x2 = cell & 16; // third bit of x

        // combine the bitpattern to grid coordinates.
        // note that the semantics of x and y are swapping
        // on each level
        int x = x0 + (x1 / 2) + (x2 / 4);
        int y = (y0 / 2) + (y1 / 4);

        if (level == 1) {
            // Root cells at north (namely "bcfguvyz") or at
            // south (namely "0145hjnp") do not have neighbors
            // in north/south direction
            if ((dy < 0 && y == 0) || (dy > 0 && y == 3)) {
                return null;
            } else {
                return Character.toString(encode(x + dx, y + dy));
            }
        } else {
            // define grid coordinates for next level
            final int nx = ((level % 2) == 1) ? (x + dx) : (x + dy);
            final int ny = ((level % 2) == 1) ? (y + dy) : (y + dx);

            // if the defined neighbor has the same parent a the current cell
            // encode the cell directly. Otherwise find the cell next to this
            // cell recursively. Since encoding wraps around within a cell
            // it can be encoded here.
            // xLimit and YLimit must always be respectively 7 and 3
            // since x and y semantics are swapping on each level.
            if (nx >= 0 && nx <= 7 && ny >= 0 && ny <= 3) {
                return geohash.substring(0, level - 1) + encode(nx, ny);
            } else {
                String neighbor = neighbor(geohash, level - 1, dx, dy);
                if(neighbor != null) {
                    return neighbor + encode(nx, ny); 
                } else {
                    return null;
                }
            }
        }
    }

    /**
     * Add all geohashes of the cells next to a given geohash to a list.
     *
     * @param geohash   Geohash of a specified cell
     * @param neighbors list to add the neighbors to
     * @return the given list
     */
    public static final <E extends Collection<? super String>> E addNeighbors(String geohash, E neighbors) {
        return addNeighbors(geohash, geohash.length(), neighbors);
    }
    
    /**
     * Add all geohashes of the cells next to a given geohash to a list.
     *
     * @param geohash   Geohash of a specified cell
     * @param length    level of the given geohash
     * @param neighbors list to add the neighbors to
     * @return the given list
     */
    public static final <E extends Collection<? super String>> E addNeighbors(String geohash, int length, E neighbors) {
        String south = neighbor(geohash, length, 0, -1);
        String north = neighbor(geohash, length, 0, +1);
        if (north != null) {
            neighbors.add(neighbor(north, length, -1, 0));
            neighbors.add(north);
            neighbors.add(neighbor(north, length, +1, 0));
        }

        neighbors.add(neighbor(geohash, length, -1, 0));
        neighbors.add(neighbor(geohash, length, +1, 0));

        if (south != null) {
            neighbors.add(neighbor(south, length, -1, 0));
            neighbors.add(south);
            neighbors.add(neighbor(south, length, +1, 0));
        }

        return neighbors;
    }

    /**
     * Decodes the given geohash
     *
     * @param geohash Geohash to decocde
     * @return {@link GeoPoint} at the center of cell, given by the geohash
     */
    public static GeoPoint decode(String geohash) {
        return decode(geohash, new GeoPoint());
    }

    public static GeoPoint decode(final String geohash, GeoPoint ret) {
        final long hash = org.apache.lucene.util.GeoHashUtils.mortonEncode(geohash);
        return ret.reset(GeoUtils.mortonUnhashLat(hash), GeoUtils.mortonUnhashLon(hash));
    }
    
    //========== long-based encodings for geohashes ========================================

    public static GeoPoint decode(long geohashAsLong) {
        int level = (int)(12 - (geohashAsLong&15));
        geohashAsLong = GeoUtils.flipFlop((geohashAsLong>>>4)<<((level*5)+2));
        return new GeoPoint(GeoUtils.mortonUnhashLat(geohashAsLong), GeoUtils.mortonUnhashLon(geohashAsLong));
    }
}
