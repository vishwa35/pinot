/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.filter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.uber.h3core.LengthUnit;

import java.util.*;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.geospatial.transform.function.StContainsFunction;
import org.apache.pinot.core.geospatial.transform.function.ScalarFunctions;
import org.apache.pinot.core.geospatial.transform.function.StDistanceFunction;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.spi.utils.BytesUtils;
import org.locationtech.jts.geom.Coordinate;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A filter operator that uses H3 index for geospatial data retrieval
 */
public class H3IndexFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "H3IndexFilterOperator";
  private static final Set<String> DISTANCE_FUNCTIONS = ImmutableSet.of(StDistanceFunction.FUNCTION_NAME.toUpperCase());
  private static final Set<String> INCLUSION_FUNCTIONS = ImmutableSet.of(StContainsFunction.FUNCTION_NAME.toUpperCase());

  private final IndexSegment _segment;
  private final Predicate _predicate;
  private final int _numDocs;
  private final H3IndexReader _h3IndexReader;
  private final long _h3Id;
  private final List<Long> _h3Ids;
  private final double _edgeLength;
  private final double _lowerBound;
  private final double _upperBound;
  private final boolean _inclusion;

  public H3IndexFilterOperator(IndexSegment segment, Predicate predicate, int numDocs) {
    _segment = segment;
    _predicate = predicate;
    _numDocs = numDocs;

    _inclusion = INCLUSION_FUNCTIONS.contains(_predicate.getLhs().getFunction().getFunctionName().toUpperCase());
    // TODO: handle nested geography/geometry conversion functions
    List<ExpressionContext> arguments = predicate.getLhs().getFunction().getArguments();
    Coordinate[] coordinates;
    if (arguments.get(0).getType() == ExpressionContext.Type.IDENTIFIER) {
      // look up arg0's h3 indices
      _h3IndexReader = segment.getDataSource(arguments.get(0).getIdentifier()).getH3Index();
      // arg1 is the literal
      coordinates = GeometrySerializer.deserialize(BytesUtils.toBytes(arguments.get(1).getLiteral())).getCoordinates();
    } else {
      // look up arg1's h3 indices
      _h3IndexReader = segment.getDataSource(arguments.get(1).getIdentifier()).getH3Index();
      // arg0 is the literal
      coordinates = GeometrySerializer.deserialize(BytesUtils.toBytes(arguments.get(0).getLiteral())).getCoordinates();
    }
    // must be some h3 index
    assert _h3IndexReader != null;

    if(!_inclusion) {
      // look up lowest resolution hexagon for the provided coordinate
      int resolution = _h3IndexReader.getH3IndexResolution().getLowestResolution();
      _h3Id = H3Utils.H3_CORE.geoToH3(coordinates[0].y, coordinates[0].x, resolution);
      // get hexagon edge length in meters for that resolution
      _edgeLength = H3Utils.H3_CORE.edgeLength(resolution, LengthUnit.m);

      // approach as range, set unbounded bounds to nan
      RangePredicate rangePredicate = (RangePredicate) predicate;
      if (!rangePredicate.getLowerBound().equals(RangePredicate.UNBOUNDED)) {
        _lowerBound = Double.parseDouble(rangePredicate.getLowerBound());
      } else {
        _lowerBound = Double.NaN;
      }
      if (!rangePredicate.getUpperBound().equals(RangePredicate.UNBOUNDED)) {
        _upperBound = Double.parseDouble(rangePredicate.getUpperBound());
      } else {
        _upperBound = Double.NaN;
      }
      _h3Ids = ImmutableList.of();
    } else {
      // look up all hexagons for provided coordinates
      List<Coordinate> coordinateList = Arrays.asList(coordinates);
      _h3Ids = ScalarFunctions.polygonToH3(coordinateList,
              ScalarFunctions.calcResFromMaxDist(ScalarFunctions.maxDist(coordinateList), 50));

      // just to silence warnings, should probably delegate to 2 diff classes for this later
      _edgeLength = 0;
      _lowerBound = 0;
      _upperBound = 0;
      _h3Id = 0L;
    }
  }

  @Override
  protected FilterBlock getNextBlock() {
    if (DISTANCE_FUNCTIONS.contains(_predicate.getLhs().getFunction().getFunctionName().toUpperCase())) {
      // verify the bounds are valid
      if (_upperBound < 0 || _lowerBound > _upperBound) {
        // Invalid upper bound, return an empty block
        return new FilterBlock(EmptyDocIdSet.getInstance());
      }

      try {
        // verify the bounds are valid
        if (Double.isNaN(_lowerBound) || _lowerBound < 0) {
          // No lower bound

          if (Double.isNaN(_upperBound)) {
            // No bound, return a match-all block
            return new FilterBlock(new MatchAllDocIdSet(_numDocs));
          }

          // Upper bound only
          List<Long> fullMatchH3Ids = getAlwaysMatchH3Ids(_upperBound);
          HashSet<Long> partialMatchH3Ids = new HashSet<>(getPossibleMatchH3Ids(_upperBound));
          partialMatchH3Ids.removeAll(fullMatchH3Ids);

          MutableRoaringBitmap fullMatchDocIds = new MutableRoaringBitmap();
          for (long fullMatchH3Id : fullMatchH3Ids) {
            fullMatchDocIds.or(_h3IndexReader.getDocIds(fullMatchH3Id));
          }

          MutableRoaringBitmap partialMatchDocIds = new MutableRoaringBitmap();
          for (long partialMatchH3Id : partialMatchH3Ids) {
            partialMatchDocIds.or(_h3IndexReader.getDocIds(partialMatchH3Id));
          }

          return getFilterBlock(fullMatchDocIds, partialMatchDocIds);
        }

        if (Double.isNaN(_upperBound)) {
          // Lower bound only

          List<Long> alwaysNotMatchH3Ids = getAlwaysMatchH3Ids(_lowerBound);
          Set<Long> possibleNotMatchH3Ids = new HashSet<>(getPossibleMatchH3Ids(_lowerBound));

          // Flip the result of possible not match doc ids to get the full match doc ids
          MutableRoaringBitmap fullMatchDocIds = new MutableRoaringBitmap();
          for (long partialMatchH3Id : possibleNotMatchH3Ids) {
            fullMatchDocIds.or(_h3IndexReader.getDocIds(partialMatchH3Id));
          }
          fullMatchDocIds.flip(0L, _numDocs);

          // Remove the always not match H3 ids from possible not match H3 ids to get the partial match H3 ids
          possibleNotMatchH3Ids.removeAll(alwaysNotMatchH3Ids);
          MutableRoaringBitmap partialMatchDocIds = new MutableRoaringBitmap();
          for (long partialMatchH3Id : possibleNotMatchH3Ids) {
            partialMatchDocIds.or(_h3IndexReader.getDocIds(partialMatchH3Id));
          }

          return getFilterBlock(fullMatchDocIds, partialMatchDocIds);
        }

        // Both lower bound and upper bound exist
        List<Long> lowerAlwaysMatchH3Ids = getAlwaysMatchH3Ids(_lowerBound);
        List<Long> lowerPossibleMatchH3Ids = getPossibleMatchH3Ids(_lowerBound);
        List<Long> upperAlwaysMatchH3Ids = getAlwaysMatchH3Ids(_upperBound);
        List<Long> upperPossibleMatchH3Ids = getPossibleMatchH3Ids(_upperBound);

        // Remove the possible match H3 ids for the lower bound from the always match H3 ids for the upper bound to get
        // the full match H3 ids
        Set<Long> fullMatchH3Ids;
        if (upperAlwaysMatchH3Ids.size() > lowerPossibleMatchH3Ids.size()) {
          fullMatchH3Ids = new HashSet<>(upperAlwaysMatchH3Ids);
          fullMatchH3Ids.removeAll(lowerPossibleMatchH3Ids);
        } else {
          fullMatchH3Ids = Collections.emptySet();
        }
        MutableRoaringBitmap fullMatchDocIds = new MutableRoaringBitmap();
        for (long fullMatchH3Id : fullMatchH3Ids) {
          fullMatchDocIds.or(_h3IndexReader.getDocIds(fullMatchH3Id));
        }

        // Remove the always match H3 ids for the lower bound (always not match H3 ids) and the full match H3 ids from the
        // possible match H3 ids for the upper bound to get the partial match H3 ids
        Set<Long> partialMatchH3Ids = new HashSet<>(upperPossibleMatchH3Ids);
        partialMatchH3Ids.removeAll(lowerAlwaysMatchH3Ids);
        partialMatchH3Ids.removeAll(fullMatchH3Ids);
        MutableRoaringBitmap partialMatchDocIds = new MutableRoaringBitmap();
        for (long partialMatchH3Id : partialMatchH3Ids) {
          partialMatchDocIds.or(_h3IndexReader.getDocIds(partialMatchH3Id));
        }

        return getFilterBlock(fullMatchDocIds, partialMatchDocIds);
      } catch (Exception e) {
        // Fall back to ExpressionFilterOperator when the execution encounters exception (e.g. numRings is too large)
        return new ExpressionFilterOperator(_segment, _predicate, _numDocs).getNextBlock();
      }
    } else if (INCLUSION_FUNCTIONS.contains(_predicate.getLhs().getFunction().getFunctionName().toUpperCase())) {
      // have list of h3 hashes for polygon provided
      // return filtered num_docs
      MutableRoaringBitmap fullMatchDocIds = new MutableRoaringBitmap();
      for (long docId : _h3Ids) {
        fullMatchDocIds.or(_h3IndexReader.getDocIds(docId));
      }
      fullMatchDocIds.flip(0L, _numDocs);

      // when h3 implements polyfill parameters for including partial matches, we can expand to include partial matches
      return getFilterBlock(fullMatchDocIds, new MutableRoaringBitmap());
    }
    throw new RuntimeException("Invalid H3 function.");
  }

  /**
   * Returns the H3 ids that is ALWAYS fully covered by the circle with the given distance as the radius and a point
   * within the _h3Id hexagon as the center.
   * <p>The farthest distance from the center of the center hexagon to the center of a hexagon in the nth ring is
   * {@code sqrt(3) * n * edgeLength}. Counting the distance from the center to a point in the hexagon, which is up
   * to the edge length, it is guaranteed that the hexagons in the nth ring are always fully covered if:
   * {@code distance >= (sqrt(3) * n + 2) * edgeLength}.
   */
  private List<Long> getAlwaysMatchH3Ids(double distance) {
    // NOTE: Pick a constant slightly larger than sqrt(3) to be conservative
    int numRings = (int) Math.floor((distance / _edgeLength - 2) / 1.7321);
    return numRings >= 0 ? getH3Ids(numRings) : Collections.emptyList();
  }

  /**
   * Returns the H3 ids that MIGHT BE fully/partially covered by the circle with the given distance as the radius and a
   * point within the _h3Id hexagon as the center.
   * <p>The shortest distance from the center of the center hexagon to the center of a hexagon in the nth ring is
   * {@code >= 1.5 * n * edgeLength}. Counting the distance from the center to a point in the hexagon, which is up
   * to the edge length, it is guaranteed that the hexagons in the nth ring are always not fully/partially covered if:
   * {@code distance < (1.5 * n - 2) * edgeLength}.
   */
  private List<Long> getPossibleMatchH3Ids(double distance) {
    // NOTE: Add a small delta (0.001) to be conservative
    int numRings = (int) Math.floor((distance / _edgeLength + 2) / 1.5 + 0.001);
    return getH3Ids(numRings);
  }

  /**
   * Returns the H3 ids for the given number of rings around the _h3Id.
   * IMPORTANT: Throw exception when number of rings is too large because H3 library might send SIGILL for large number
   * of rings, which can terminate the JVM. Also, the result won't be accurate when the distance is too large. In such
   * case, the operator will fallback to ExpressionFilterOperator.
   */
  private List<Long> getH3Ids(int numRings) {
    Preconditions.checkState(numRings <= 100, "Expect numRings <= 100, got: %s", numRings);
    return H3Utils.H3_CORE.kRing(_h3Id, numRings);
  }

  /**
   * Returns the filter block based on the given full match doc ids and the partial match doc ids.
   */
  private FilterBlock getFilterBlock(MutableRoaringBitmap fullMatchDocIds, MutableRoaringBitmap partialMatchDocIds) {
    ExpressionFilterOperator expressionFilterOperator = new ExpressionFilterOperator(_segment, _predicate, _numDocs);
    ScanBasedDocIdIterator docIdIterator =
        (ScanBasedDocIdIterator) expressionFilterOperator.getNextBlock().getBlockDocIdSet().iterator();
    MutableRoaringBitmap result = docIdIterator.applyAnd(partialMatchDocIds);
    result.or(fullMatchDocIds);
    return new FilterBlock(new BitmapDocIdSet(result, _numDocs) {
      @Override
      public long getNumEntriesScannedInFilter() {
        return docIdIterator.getNumEntriesScanned();
      }
    });
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
