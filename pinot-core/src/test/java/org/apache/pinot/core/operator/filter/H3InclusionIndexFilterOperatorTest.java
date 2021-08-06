package org.apache.pinot.core.operator.filter;

import com.google.common.collect.ImmutableList;
import com.uber.h3core.util.GeoCoord;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.geospatial.transform.function.ScalarFunctions;
import org.apache.pinot.core.geospatial.transform.function.StContainsFunction;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Coordinate;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class H3InclusionIndexFilterOperatorTest {

    private H3InclusionIndexFilterOperator filter;
    private static final int NUM_DOCS = 100;
    private static final String COL = "geo";
    private static final String LITERAL = Hex.encodeHexString(GeometrySerializer.serialize(GeometryUtils.GEOGRAPHY_FACTORY.createPoint(new Coordinate(20,20))));

    @Mock
    private IndexSegment indexSegment;

    @Spy
    private Predicate predicate;

    @Mock
    private DataSource dataSource;

    @Mock
    private DataSourceMetadata dataSourceMetadata;

    @Mock
    private H3IndexReader h3IndexReader;

    @Mock
    private FunctionContext functionContext;

    @BeforeMethod
    public void setup() {
        initMocks(this);
        when(indexSegment.getDataSource(COL)).thenReturn(dataSource);
        when(dataSource.getH3Index()).thenReturn(h3IndexReader);
        List<ExpressionContext> expressionContextList = ImmutableList.<ExpressionContext>builder()
                .add(ExpressionContext.forIdentifier(COL))
                .add(ExpressionContext.forLiteral(LITERAL))
                .build();
//        when(predicate.getLhs()).thenReturn(ExpressionContext.forFunction(functionContext));
        predicate = new EqPredicate(ExpressionContext.forFunction(functionContext), String.valueOf(1));
        when(functionContext.getArguments()).thenReturn(expressionContextList);
        filter = new H3InclusionIndexFilterOperator(indexSegment, predicate, NUM_DOCS);
    }

    @Test
    public void test() {
        when(functionContext.getFunctionName()).thenReturn(StContainsFunction.FUNCTION_NAME);
        when(dataSource.getDictionary()).thenReturn(null);
        when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
        when(dataSourceMetadata.getDataType()).thenReturn(FieldSpec.DataType.BYTES);
        when(dataSourceMetadata.isSingleValue()).thenReturn(true);
//        when(predicate.getType()).thenReturn(Predicate.Type.IN);
        doAnswer((Answer<Void>) invocationOnMock -> {
            ((Set<String>) invocationOnMock.getArgument(0)).add(COL);
            return null;
        }).when(functionContext).getColumns(anySet());

        filter.getNextBlock();
    }

    @Test
    public void algo() {
        // test POINT (25 25) IN POLYGON ((20 20, 30 30)) only gives relevant docIds
        int res = ScalarFunctions.calcResFromMaxDist(ScalarFunctions.maxDist(ImmutableList.of(new Coordinate(20,20), new Coordinate(30,30))), 5);
        Assert.assertEquals(res, 15);
        long expectedIndex = H3Utils.H3_CORE.geoToH3(25, 25, res);
        Set<Long> fullMatch = new HashSet<>(H3Utils.H3_CORE.polyfill(ImmutableList.of(new GeoCoord(20,20), new GeoCoord(20,30), new GeoCoord(30,30), new GeoCoord(30,20)), ImmutableList.of(), 10));
        Set<Long> partialMatch = fullMatch.stream().map(match -> H3Utils.H3_CORE.kRing(match, 1)).flatMap(List::stream).collect(Collectors.toUnmodifiableSet());
        Set<Long> allMatch = new HashSet<>();
        allMatch.addAll(fullMatch);
        allMatch.addAll(partialMatch);

        Assert.assertEquals(fullMatch.size(), 1);
        Assert.assertEquals(expectedIndex, 1L);
        Assert.assertTrue(allMatch.contains(expectedIndex));


    }
}