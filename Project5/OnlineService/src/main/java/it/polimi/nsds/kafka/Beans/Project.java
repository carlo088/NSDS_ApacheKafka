package it.polimi.nsds.kafka.Beans;

public class Project {
    private String id;
    private final String description;
    private final String courseId;

    public Project(String id, String description, String courseId) {
        this.id = id;
        this.description = description;
        this.courseId = courseId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id){
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public String getCourseId() {
        return courseId;
    }
}
