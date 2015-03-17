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
//import java.util.*;
//
//public class MyTest {
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
//        geomesaParams.put("catalog", "andrew_twitter2");
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
//        long parsed = 0;
//        while(itr.hasNext()){
//            SimpleFeature sf = itr.next();
//
//            Long id = (Long) sf.getAttribute("user_id");
//            Point p = (Point) sf.getDefaultGeometry();
//            Double lat = p.getCoordinate().getOrdinate(1);
//
//            if(latMap.containsKey(id)){
//                latMap.get(id).add(lat);
//            }
//            else{
//                List<Double> lst = Lists.<Double>newLinkedList();
//                lst.add(lat);
//                latMap.put(id, lst);
//            }
//
//            parsed = parsed +1;
//            if(parsed % 50_000 == 0){
//                System.out.println("Parsed "+parsed);
//            }
//        }
//
//        List<Tuple> stdTop = new LinkedList<>();
//        List<Tuple> countTop = new LinkedList<>();
//
//        for(Long id: latMap.keySet()){
//            Statistics stats = new Statistics(latMap.get(id));
//
//            double std = stats.getDistance();
//            double size = latMap.get(id).size();
//            checkAndAdd(id, std, stdTop, 20);
//            checkAndAdd(id, size, countTop, 20);
//        }
//
//        System.out.println("STD:");
//        List<String> qs = new ArrayList<>();
//        for(Tuple t: stdTop){
//            String s = "user_id="+Double.toString(t.value);
//            qs.add(s);
//            System.out.println(Long.toString(t.key )+ " " + Double.toString(t.value));
//        }
//        System.out.println(Joiner.on(" OR ").join(qs));
//
//        System.out.println();
//
//        System.out.println("count:");
//        List<String> cs = new ArrayList<>();
//        for(Tuple t: countTop){
//            String s = "user_id="+Double.toString(t.value);
//            cs.add(s);
//            System.out.println(Long.toString(t.key )+ " " + Double.toString(t.value));
//        }
//        System.out.println(Joiner.on(" OR ").join(cs));
//
//
//    }
//
//    public static void checkAndAdd(long id, double val, List<Tuple> list, int max) {
//        if(list.size() >= max){
//            ListIterator<Tuple> lt = list.listIterator();
//            while(lt.hasNext()){
//                Tuple t = lt.next();
//                if(t.value < val){
//                    lt.remove();
//                    lt.add(new Tuple(id, val));
//                    break;
//                }
//            }
//        }
//        else {
//            list.add(new Tuple(id, val));
//        }
//    }
//
//    static class Tuple{
//        public Tuple(long key, double value){
//            this.key = key;
//            this.value = value;
//        }
//        long key;
//        double value;
//    }
//
//
//
//    static class Statistics
//    {
//        List<Double> data;
//        double size;
//
//        public Statistics(List<Double> data)
//        {
//            this.data = data;
//            size = data.size();
//        }
//
//        double getMean()
//        {
//            double sum = 0.0;
//            for(double a : data)
//                sum += a;
//                return sum/size;
//        }
//
//        double getVariance()
//        {
//            double mean = getMean();
//            double temp = 0;
//            for(double a :data)
//                temp += (mean-a)*(mean-a);
//                return temp/size;
//        }
//
//        double getStdDev()
//        {
//            return Math.sqrt(getVariance());
//        }
//
//        double getDistance()
//        {
//            double min = Double.MAX_VALUE;
//            double max = Double.MIN_VALUE;
//            for(double a: data){
//                if(a > max) max = a;
//                if(a < min) min = a;
//            }
//            return max-min;
//        }
//
//
//    }
//
//
//}
