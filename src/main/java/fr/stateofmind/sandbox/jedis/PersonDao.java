package fr.stateofmind.sandbox.jedis;

import com.google.common.collect.ImmutableMap;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class PersonDao {

    private static final String NAME = "name";
    private static final String AGE = "age";

    private Jedis jedis;


    public PersonDao(Jedis jedis) {
        this.jedis = jedis;
    }

    public void insertPerson(Person person) {
        Map<String, String> fields = ImmutableMap.<String, String>builder()
                                                 .put(NAME, person.getName())
                                                 .put(AGE, Integer.toString(person.getAge())).build();

        jedis.hmset("person:" + person.getName().toLowerCase(), fields);
    }

    public Person findPersonByKey(String key) {
        Map<String, String> fields = jedis.hgetAll(key);
        return new Person(fields.get(NAME), Integer.parseInt(fields.get(AGE)));
    }
}
