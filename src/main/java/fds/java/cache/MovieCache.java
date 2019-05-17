package fds.java.cache;

import com.zink.cache.Cache;
import com.zink.cache.CacheFactory;

import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/**
 * Stub for a Movie Cache
 */
public class MovieCache {

    static final String base = "https://api.themoviedb.org/3/";
    // TODO replace ??? with real key to access the movie database
    static final String key = "?api_key=???";

    static Cache cache = CacheFactory.connect("127.0.0.1");

    public static void main(String[] args) throws Exception {
        List<String> ids = getPopularMovieIDs();
        System.out.println( ids );
        System.out.println( getDetailsById( ids.get(0) ) );

    }

    public static List<String> getPopularMovieIDs( ) throws Exception {
        String pop = "movie/popular";
        String json =  httpGet(base + pop + key);
        List<String> ids = scanForKeysValue( "\"id\":", json );
        Collections.reverse(ids);       // most popular first
        return ids;
    }

    public static List<String> scanForKeysValue(String pattern, String object ) {
        int idx = object.indexOf(pattern);
        if (idx == -1)  {
            return new ArrayList<>();
        } else {
            String valueAndTail = object.substring(idx+pattern.length());
            List<String> ids = scanForKeysValue(pattern, valueAndTail);
            ids.add(valueAndTail.split(",")[0]);
            return ids;
        }
    }

    public static String getDetailsById( String  id ) throws Exception  {
        Object cached = cache.get(id);
        if (cached == null) {
            String movie = "movie/";
            String result =  httpGet(base + movie + id + key);
            cache.set(id, result);
            return result;
        } else
             return cached.toString();
    }


    public static String httpGet(String uri) throws Exception {
        URLConnection conn = new URL(uri).openConnection();
        Scanner scr = new Scanner(conn.getInputStream());
        scr.useDelimiter("\\Z");
        return scr.next();
    }

}
