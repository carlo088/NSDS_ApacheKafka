package it.polimi.nsds.kafka.Beans;

public class Course {
    private String id;
    private final String name;
    private final int cfu;
    private final int projectNum;
    private final String professor;

    public Course(String id, String name, int cfu, int projectNum, String professor) {
        this.id = id;
        this.name = name;
        this.cfu = cfu;
        this.projectNum = projectNum;
        this.professor = professor;
    }

    public String getId() {
        return id;
    }

    public void setId(String id){
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public int getCfu() {
        return cfu;
    }

    public int getProjectNum() {
        return projectNum;
    }

    public String getProfessor() {
        return professor;
    }
}
