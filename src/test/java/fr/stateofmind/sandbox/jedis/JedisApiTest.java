package fr.stateofmind.sandbox.jedis;

import static com.google.common.collect.Lists.*;
import static com.google.common.collect.Sets.*;
import static org.junit.Assert.*;

import com.google.common.collect.ImmutableMap;
import org.junit.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisDataException;

public class JedisApiTest {

    private static Jedis jedis;

    @BeforeClass public static void setUpClass() throws Exception {
        jedis = new Jedis("localhost", 6379);
        jedis.connect();
        System.out.println("Connected");
    }

    @AfterClass public static void tearDownClass() throws Exception {
        jedis.quit();
        System.out.println("Disconnected");
    }

    @Test public void simpleMap() throws Exception {
        jedis.set("map:string", "Value");

        assertEquals("Value", jedis.get("map:string"));
    }

    @Test public void simpleMap_withIntegerValue() throws Exception {
        jedis.set("map:int", "0");
        jedis.incr("map:int");

        assertEquals("1", jedis.get("map:int"));
    }

    @Test(expected = JedisDataException.class)
    public void simpleMap_withStringFailOnIncr() throws Exception {
        jedis.set("map:string", "Value");
        jedis.incr("map:string");
    }

    @Test public void mapOfMap_withIntegerValue() throws Exception {
        jedis.hset("user", "firstname", "Olivier");
        jedis.hset("user", "age", "35");

        assertEquals("Olivier", jedis.hget("user", "firstname"));

        assertEquals(ImmutableMap.<String, String>builder().put("firstname", "Olivier").put("age", "35").build(),
                     jedis.hgetAll("user"));
    }

    @Test public void mapOfMap_withPipeline() throws Exception {
        jedis.pipelined(new PipelineBlock() {
            @Override
            public void execute() {
                hset("product_1", "name", "iphone");
                hset("product_1", "price", "499");
                hset("product_1", "network", "4G");

                hset("product_2", "name", "ipad");
                hset("product_2", "price", "509");
                hset("product_2", "color", "white");
            }
        });

        assertEquals(ImmutableMap.<String, String>builder()
                                 .put("name", "iphone")
                                 .put("price", "499")
                                 .put("network", "4G")
                                 .build(),
                     jedis.hgetAll("product_1"));
        assertEquals(ImmutableMap.<String, String>builder()
                                 .put("name", "ipad")
                                 .put("price", "509")
                                 .put("color", "white")
                                 .build(),
                     jedis.hgetAll("product_2"));
    }

    @Test public void mapOfList() throws Exception {
        jedis.del("languages");
        jedis.rpush("languages", "Java");
        jedis.rpush("languages", "Scala");
        jedis.rpush("languages", "Python");
        jedis.rpush("languages", "Ruby");
        jedis.rpush("languages", "Clojure");

        assertEquals(5, jedis.llen("languages").longValue());
        assertEquals(newArrayList("Java", "Scala", "Python", "Ruby", "Clojure"),
                     jedis.lrange("languages", 0, -1));
        assertEquals(newArrayList("Java", "Scala", "Python", "Ruby", "Clojure"),
                     jedis.lrange("languages", 0, Integer.MAX_VALUE));
        assertEquals(newArrayList("Java", "Scala", "Python"),
                     jedis.lrange("languages", 0, 2));

        assertEquals("Java", jedis.lpop("languages"));
        assertEquals("Clojure", jedis.rpop("languages"));
        assertEquals(newArrayList("Scala", "Python", "Ruby"),
                     jedis.lrange("languages", 0, -1));
    }

    @Test public void mapOfSet() throws Exception {
        jedis.sadd("product", "Book");
        jedis.sadd("product", "Phone");
        jedis.sadd("product", "Computer");
        jedis.sadd("product", "Phone");

        assertEquals(3, jedis.scard("product").longValue());
        assertEquals(newHashSet("Book", "Phone", "Computer"),
                     jedis.smembers("product"));
    }

    @Test public void mapOfSet_union_diff_inter() throws Exception {
        jedis.sadd("a", "Book");
        jedis.sadd("a", "Computer");
        jedis.sadd("a", "Tablet");

        jedis.sadd("b", "Phone");
        jedis.sadd("b", "Computer");

        assertEquals(newHashSet("Book", "Phone", "Computer", "Tablet"), jedis.sunion("a", "b"));
        assertEquals(newHashSet("Book", "Tablet"), jedis.sdiff("a", "b"));
        assertEquals(newHashSet("Computer"), jedis.sinter("a", "b"));

        Long unionStatus = jedis.sunionstore("unionstore", "a", "b");
        assertEquals(4, unionStatus.longValue());
        assertEquals(newHashSet("Book", "Phone", "Computer", "Tablet"), jedis.smembers("unionstore"));

        Long diffStatus = jedis.sdiffstore("diffstore", "a", "b");
        assertEquals(2, diffStatus.longValue());
        assertEquals(newHashSet("Book", "Tablet"), jedis.smembers("diffstore"));

        jedis.sinterstore("interstore", "a", "b");
        assertEquals(newHashSet("Computer"), jedis.smembers("interstore"));
    }

    @Test public void expire() throws Exception {
        jedis.set("will:expire", "temporary");
        jedis.expire("will:expire", 1);

        assertEquals("temporary", jedis.get("will:expire"));
        Thread.sleep(1200);
        assertNull(jedis.get("will:expire"));
    }

    @Test public void mapOfSortedSet() throws Exception {
        jedis.zadd("person", 10.0, "Bill");
        jedis.zadd("person", 40.0, "Bob");
        jedis.zadd("person", 50.0, "Eddy");
        jedis.zadd("person", 10.0, "John");

        assertEquals(4, jedis.zcard("person").longValue());
        assertEquals(3, jedis.zcount("person", 10.0, 40.0).longValue());
        assertEquals(newHashSet("John", "Bob"), jedis.zrange("person", 1, 2));
        assertEquals(newHashSet("Bill", "John", "Bob"), jedis.zrangeByScore("person", 10.0, 40.0));
        assertEquals(newHashSet("Bob", "John", "Bill"), jedis.zrevrange("person", 1, 3));
        assertEquals(newHashSet("Bob", "John", "Bill"), jedis.zrangeByScore("person", 10.0, 40.0));
        assertEquals(2, jedis.zrank("person", "Bob").longValue());
        assertEquals(1, jedis.zrevrank("person", "Bob").longValue());
        assertEquals(new Double(40.0), jedis.zscore("person", "Bob"));
        assertEquals(newHashSet(new Tuple("John", 10.0), new Tuple("Bob", 40.0)),
                     jedis.zrangeWithScores("person", 1, 2));

        Double newScore = jedis.zincrby("person", 25.0, "John");
        assertEquals(new Double(35.0), newScore);
        assertEquals(new Double(35.0), jedis.zscore("person", "John"));

        jedis.zrem("person", "Bill");
        assertEquals(newHashSet("John", "Bob", "Eddy"), jedis.zrange("person", 0, -1));
    }
}
