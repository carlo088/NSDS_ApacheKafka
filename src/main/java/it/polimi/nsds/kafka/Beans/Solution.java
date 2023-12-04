package it.polimi.nsds.kafka.Beans;

public class Solution {
    private final String username;
    private final String id;
    private final String solutionText;

    public Solution(String username, String id, String solutionText) {
        this.username = username;
        this.id = id;
        this.solutionText = solutionText;
    }
    
}

