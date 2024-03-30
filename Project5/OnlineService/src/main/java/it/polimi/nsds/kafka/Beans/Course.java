package it.polimi.nsds.kafka.Beans;

import java.util.List;

public class Course {
    private String id;
    private final String name;
    private final int cfu;
    private final int projectNum;
    private final List<String> projectIds;
    private boolean old;

    public Course(String id, String name, int cfu, int projectNum, List<String> projectIds) {
        this.id = id;
        this.name = name;
        this.cfu = cfu;
        this.projectNum = projectNum;
        this.projectIds = projectIds;
        this.old = false;
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

    public List<String> getProjectIds() {
        return projectIds;
    }

    public void setOld(boolean old) {
        this.old = old;
    }

    public boolean isOld() {
        return old;
    }
}
