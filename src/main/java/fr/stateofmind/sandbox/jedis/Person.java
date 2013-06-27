package fr.stateofmind.sandbox.jedis;

import com.google.common.base.Objects;


public class Person {
    private String name;
    private int age;


    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Person that = (Person) obj;

        return Objects.equal(this.name, that.name)
               && Objects.equal(this.age, that.age);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, age);
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("age", age)
                      .toString();
    }
}
