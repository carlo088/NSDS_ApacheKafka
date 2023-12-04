package it.polimi.nsds.kafka.Beans;

import java.util.List;

public class User {
    private final String username;
    private final String password;
    private final String role;
    private final List<String> courses;

    public User(String username, String password, String role, List<String> courses) {
        this.username = username;
        this.password = password;
        this.role = role;
        this.courses = courses;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getRole() {
        return role;
    }

    public List<String> getCourses() {
        return courses;
    }

    public void addCourse(String course){
        courses.add(course);
    }
}
