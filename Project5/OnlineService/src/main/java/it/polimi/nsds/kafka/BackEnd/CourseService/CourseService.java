package it.polimi.nsds.kafka.BackEnd.CourseService;

import com.google.gson.Gson;
import it.polimi.nsds.kafka.Beans.Course;
import it.polimi.nsds.kafka.Beans.Project;
import it.polimi.nsds.kafka.Utils.ConfigUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;

public class CourseService implements Runnable{
    // socket and streams
    private final Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    private boolean isActive = true;

    // users and courses stored in private data structures
    private final Map<String, String> db_courses;
    private final Map<String, String> db_projects;

    // kafka producer
    private final KafkaProducer<String, String> courseProducer;

    public CourseService(Socket socket, Map<String, String> db_courses, Map<String, String> db_projects){
        this.socket = socket;
        this.db_courses = db_courses;
        this.db_projects = db_projects;

        courseProducer = setCourseProducer();
    }

    /**
     * starts the main process of the service (receiving messages from the client)
     */
    @Override
    public void run() {
        try {
            // set socket streams
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            send(new String[]{"Connection to Course Service established!"});

            // service is always waiting for requests from clients
            while(isActive){
                receive();
            }
        } catch(IOException e){
            System.err.println("Connection closed from client side");
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    /**
     * sends a response message to the client through the socket's output stream
     * @param message response message
     */
    public void send(String[] message){
        try {
            if (!isActive) {
                return;
            }
            out.writeObject(message);
            out.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * closes the connection
     */
    public synchronized void closeConnection(){
        try{
            socket.close();
        }catch (IOException e){
            System.err.println(e.getMessage());
        }
        System.out.println("Connection closed");
        isActive = false;
    }

    /**
     * receives a request message from the client through the socket's input stream (BLOCKING FUNCTION)
     * @throws IOException if there are IO problems
     * @throws ClassNotFoundException if there are problems with readObject() method
     */
    private void receive() throws IOException, ClassNotFoundException {
        String[] request = (String[]) in.readObject();
        String requestType = request[0];

        switch(requestType){
            case "POST":
                String[] response = newProject(request[1]);
                send(response);
                break;
            case "ADD_COURSE":
                response = newCourse(request[1]);
                send(response);
                break;
            case "SHOW_ALL_COURSES":
                response = showAllCourses();
                send(response);
                break;
            case "REMOVE_COURSE":
                response = removeCourse(request[1]);
                send(response);
                break;
            case "SHOW_COURSE_PROJECTS":
                response = showCourseProjects(request[1]);
                send((response));
                break;
            default:
                send(new String[]{""});
                break;
        }
    }

    /**
     * adds a new course
     * @param courseJson course json
     * @return message for the client
     */
    public String[] newCourse(String courseJson) {
        // get a Course class from a Json file
        Gson gson = new Gson();
        Course course = gson.fromJson(courseJson, Course.class);

        //generate key
        Random rand = new Random();
        String id = null;
        boolean valid = false;
        while(!valid){
            int randId = rand.nextInt(1001);
            id = String.valueOf(randId);
            if(!db_courses.containsKey(id))
                valid = true;
        }

        course.setId(id);
        db_courses.put(id, gson.toJson(course));
        final ProducerRecord<String, String> courseRecord = new ProducerRecord<>("courses", id, gson.toJson(course));
        courseProducer.send(courseRecord);
        return new String[]{"Course added correctly"};
    }

    /**
     * removes a course setting it as "old"
     * @param courseId course id
     * @return message for the client
     */
    public synchronized String[] removeCourse(String courseId){
        if (!db_courses.containsKey(courseId))
            return new String[]{"Course doesn't exists"};

        Gson gson = new Gson();
        String courseJson = db_courses.get(courseId);
        Course course = gson.fromJson(courseJson, Course.class);
        course.setOld(true);
        final ProducerRecord<String, String> courseRecord = new ProducerRecord<>("courses", courseId, gson.toJson(course));
        courseProducer.send(courseRecord);
        db_courses.remove(courseId);

        return new String[]{"Course removed correctly"};
    }

    /**
     * shows all existing courses
     * @return all the courses in json format
     */
    public String[] showAllCourses(){
        String response = "";
        for (String courseJson: db_courses.values()) {
            response += courseJson + " ";
        }
        return response.split(" ");
    }

    /**
     * create a new project and add it to the course list
     * @param projectJson project json
     * @return message for the client
     */
    public synchronized String[] newProject(String projectJson){
        // get a Project class from a Json file
        Gson gson = new Gson();
        Project project = gson.fromJson(projectJson, Project.class);

        if(!db_courses.containsKey(project.getCourseId()))
            return new String[]{"Course doesn't exists"};

        Course course = gson.fromJson(db_courses.get(project.getCourseId()), Course.class);
        if (course.getProjectIds().size() == course.getProjectNum())
            return new String[]{"You can't add more than " + course.getProjectNum() + " projects to this course"};

        //generate key
        Random rand = new Random();
        String id = null;

        boolean valid = false;
        while(!valid){
            int randId = rand.nextInt(1001);
            id = String.valueOf(randId);
            if(!db_projects.containsKey(id))
                valid = true;
        }

        project.setId(id);
        course.getProjectIds().add(id);

        // update course list on local db and Kafka
        db_courses.put(course.getId(), gson.toJson(course));
        final ProducerRecord<String, String> courseRecord = new ProducerRecord<>("courses", course.getId(), gson.toJson(course));
        courseProducer.send(courseRecord);

        // public project on local db and Kafka
        db_projects.put(project.getId(), gson.toJson(project));
        final ProducerRecord<String, String> projectRecord = new ProducerRecord<>("projects", id, gson.toJson(project));
        courseProducer.send(projectRecord);
        return new String[]{"Project correctly posted"};
    }

    /**
     * shows all the projects for a course
     * @param courseId course id
     * @return message for the client
     */
    public String[] showCourseProjects(String courseId){
        if (!db_courses.containsKey(courseId))
            return new String[]{"Course doesn't exists"};

        Gson gson = new Gson();
        List<String> projects = new ArrayList<>();
        Course course = gson.fromJson(db_courses.get(courseId), Course.class);

        for (String projectId: course.getProjectIds()) {
            String projectJson = db_projects.get(projectId);
            projects.add(projectJson);
        }

        String[] response = new String[projects.size()];
        projects.toArray(response);
        return response;
    }

    /**
     * Kafka settings for Course Producer
     * @return Course Producer
     */
    private static KafkaProducer<String, String> setCourseProducer(){
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.kafkaBrokers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }
}
