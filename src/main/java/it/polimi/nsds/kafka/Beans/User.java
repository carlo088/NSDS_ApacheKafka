package it.polimi.nsds.kafka.Beans;

import java.util.List;

public class User {
    private final String username;
    private final String password;
    private final String role;
    private final List<String> courseIds;

    public User(String username, String password, String role, List<String> courseIds) {
        this.username = username;
        this.password = password;
        this.role = role;
        this.courseIds = courseIds;
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

    public List<String> getCourseIds() {
        return courseIds;
    }

    public void addEnrolledCourse(String courseId) {
        courseIds.add(courseId);
    }
}
