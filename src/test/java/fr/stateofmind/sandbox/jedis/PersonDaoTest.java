package fr.stateofmind.sandbox.jedis;

import static com.lordofthejars.nosqlunit.redis.EmbeddedRedis.EmbeddedRedisRuleBuilder.*;
import static com.lordofthejars.nosqlunit.redis.RedisRule.RedisRuleBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.redis.EmbeddedRedis;
import com.lordofthejars.nosqlunit.redis.RedisRule;
import org.junit.*;
import redis.clients.jedis.Jedis;

import javax.inject.Inject;

public class PersonDaoTest {

    @ClassRule public static EmbeddedRedis embeddedRedis = newEmbeddedRedisRule().build();

    @Rule public RedisRule redisRule = newRedisRule().defaultEmbeddedRedis();

    @Inject public Jedis jedis;

    @Test
    @UsingDataSet(locations = "person-input.json", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    public void findPersonByKey() {
        PersonDao personDao = new PersonDao(jedis);
        Person person = personDao.findPersonByKey("person:olivier");

        assertThat(person, is(new Person("Olivier", 35)));
    }

    @Test
    @UsingDataSet(locations = "person-input.json", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    @ShouldMatchDataSet(location = "person-output.json")
    public void insertPerson() {
        PersonDao personDao = new PersonDao(jedis);
        personDao.insertPerson(new Person("Junior", 5));
    }
}
