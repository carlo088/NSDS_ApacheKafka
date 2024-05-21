package it.polimi.nsds.kafka.Beans;

import java.util.List;

public class User {
    private final String username;
    private final String password;
    private final String role;
    private final List<String> courseIDs;

    public User(String username, String password, String role, List<String> courseIDs) {
        this.username = username;
        this.password = password;
        this.role = role;
        this.courseIDs = courseIDs;
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

    public List<String> getCourseIDs() {
        return courseIDs;
    }

    public void addCourseID(String courseID){
        courseIDs.add(courseID);
    }
}
