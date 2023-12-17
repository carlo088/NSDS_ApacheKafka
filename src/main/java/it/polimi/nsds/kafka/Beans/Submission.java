package it.polimi.nsds.kafka.Beans;

public class Submission {
    private String id;
    private String projectId;
    private String studentUsername;
    private String solution;
    private int grade;

    public Submission(String id, String projectId, String studentUsername, String solution, int grade) {
        this.id = id;
        this.projectId = projectId;
        this.studentUsername = studentUsername;
        this.solution = solution;
        this.grade = -1;
    }

    public String getId() {
        return id;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getStudentUsername() {
        return studentUsername;
    }

    public int getGrade() {
        return grade;
    }

    public String getSolution() {
        return solution;
    }

    public void setGrade(int grade) {
        this.grade = grade;
    }

    public void setId(String id) {
        this.id = id;
    }
}
