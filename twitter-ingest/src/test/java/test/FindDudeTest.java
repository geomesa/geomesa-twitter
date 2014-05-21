//package test;
//
//
//import com.google.common.base.Joiner;
//import com.google.common.collect.Lists;
//import com.vividsolutions.jts.geom.Point;
//import geomesa.core.data.AccumuloDataStore;
//import geomesa.core.data.AccumuloFeatureStore;
//import org.geotools.data.DataStoreFinder;
//import org.geotools.data.DataUtilities;
//import org.geotools.data.simple.SimpleFeatureIterator;
//import org.geotools.factory.CommonFactoryFinder;
//import org.geotools.referencing.CRS;
//import org.geotools.referencing.crs.DefaultGeographicCRS;
//import org.junit.Test;
//import org.opengis.feature.simple.SimpleFeature;
//import org.opengis.feature.simple.SimpleFeatureType;
//import org.opengis.filter.Filter;
//import org.opengis.filter.FilterFactory2;
//
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
//public class FindDudeTest {
//
//    @Test
//    public void test() throws  Exception{
//
//        SimpleFeatureType type = DataUtilities.createType("twitter2", "tweet_id:java.lang.Long,user_name:String,user_id:java.lang.Long,text:String,dtg:Date,geom:Geometry:srid=4326");
//
//        final Map<String, String> geomesaParams = new HashMap<>();
//        geomesaParams.put("instanceId", "dcloud");
//        geomesaParams.put("zookeepers", "dzoo1,dzoo2,dzoo3");
//        geomesaParams.put("user", "root");
//        geomesaParams.put("password", "secret");
//        geomesaParams.put("tableName", "andrew_twitter2");
//
//        AccumuloDataStore ds = (AccumuloDataStore) DataStoreFinder.getDataStore(geomesaParams);
//        ds.createSchema(type);
//
//        AccumuloFeatureStore source = (AccumuloFeatureStore) ds.getFeatureSource("twitter2");
//
//        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
//        Filter f = ff.bbox("geom", -77.3,38.75,-74.76,40.37, CRS.toSRS(DefaultGeographicCRS.WGS84));
//        //Filter filter = ECQL.toFilter("bbox(-77.3,38.75,-74.76,40.37)");
//        SimpleFeatureIterator itr = source.getFeatures(f).features();
//
//        final Map<Long, List<Double>> latMap= new HashMap<>();
//
//        TimeZone tz = TimeZone.getTimeZone("UTC");
//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//        df.setTimeZone(tz);
//
//        long parsed = 0;
//        while(itr.hasNext()){
//            SimpleFeature sf = itr.next();
//
//            Long id = (Long) sf.getAttribute("user_id");
//            Point p = (Point) sf.getDefaultGeometry();
//            Double lat = p.getCoordinate().getOrdinate(1);
//
//            if(sf.getAttribute("user_id").equals(114310069L)){
//                System.out.println("( \"[" + p.getCoordinate().getOrdinate(0) + " , " + p.getCoordinate().getOrdinate(1) + "]\" , \"" + df.format((Date)sf.getAttribute("dtg")) + "\")");
//            }
//        }
//
//
//
//
//    }
//
//
//}
