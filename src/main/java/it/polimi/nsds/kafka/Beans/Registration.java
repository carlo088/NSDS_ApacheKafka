package it.polimi.nsds.kafka.Beans;

public class Registration {
    private String id;
    private String username;
    private String course;
    private int grade;

    public Registration(String id, String username, String course, int grade) {
        this.id = id;
        this.username = username;
        this.course = course;
        this.grade = grade;
    }

    public String getId() {
        return id;
    }

    public String getUsername() {
        return username;
    }

    public String getCourse() {
        return course;
    }

    public int getGrade() {
        return grade;
    }
}
