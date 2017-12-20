/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.book;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString((new Random()).nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    private volatile Map<String, BigDecimal> values;
    private volatile BigDecimal result;
    private volatile String returnString;
    private volatile int counter;

    /*
     * In general, it is not a good idea to block the callback thread
     * of the ZooKeeper client. We use a thread pool executor to detach
     * the computation from the callback.
     */
    private ThreadPoolExecutor executor;

    /**
     * Creates a new Worker instance.
     *
     * @param hostPort
     */
    public Worker(String hostPort) {
        this.hostPort = hostPort;
        this.values = new HashMap<>();
        this.result = null;
        this.returnString = null;
        this.counter = 0;
        this.executor = new ThreadPoolExecutor(1, 1,
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(200));
    }

    enum TaskType {INSERT, RETRIEVE, DELETE, SUBMIT, INVALID}

    /**
     * Creates a ZooKeeper session.
     *
     * @throws IOException
     */
    public void startZK() throws IOException {
        //Connects worker to zookeeper server.
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    /**
     * Deals with session events like connecting
     * and disconnecting.
     *
     * @param e new event generated
     */
    public void process(WatchedEvent e) {
        LOG.info(e.toString() + ", " + hostPort);
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
                case SyncConnected:
                /*
                 * Registered with ZooKeeper
                 */
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expired");
                default:
                    break;
            }
        }
    }

    /**
     * Checks if this client is connected.
     *
     * @return boolean
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * Checks if ZooKeeper session is expired.
     *
     * @return
     */
    public boolean isExpired() {
        return expired;
    }

    /**
     * Bootstrapping here is just creating a /assign parent
     * znode to hold the tasks assigned to this worker.
     */
    public void bootstrap(){
        createAssignNode();
    }

    void createAssignNode(){
        /*
         * Creates the znode in the assign znode whose name matches the workers
         * own id. Used by the master to assign task, and the worker will know it has
         * been assigned a task because it is watching only the znode it just created.
         */
        zk.create("/assign/worker-" + serverId, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createAssignCallback, null);
    }

    StringCallback createAssignCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                    createAssignNode();
                    break;
                case OK:
                    LOG.info("Assign node created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Assign node already registered");
                    break;
                default:
                    LOG.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    String name;

    /**
     * Registering the new worker, which consists of adding a worker
     * znode to /workers.
     */
    public void register(){
        name = "worker-" + serverId;

        /*
         * Creates a new worker znode in the workers znode. This allows the master
         * to know which workers are available to receive tasks. Ephemeral so that
         * when worker dies, the znode disappears as well.
         */
        zk.create("/workers/" + name,
                "Idle".getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }

    StringCallback createWorkerCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                    register();

                    break;
                case OK:
                    LOG.info("Registered successfully: " + serverId);

                    break;
                case NODEEXISTS:
                    LOG.warn("Already registered: " + serverId);

                    break;
                default:
                    LOG.error("Something went wrong: ",
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    StatCallback statusUpdateCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String)ctx);
                    return;
            }
        }
    };

    String status;
    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            /*
             * Updates the status of the znode to connectionloss if it loses
             * a connection.
             */
            zk.setData("/workers/" + name, status.getBytes(), -1,
                    statusUpdateCallback, status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    private int executionCount;

    synchronized void changeExecutionCount(int countChange) {
        executionCount += countChange;
        if (executionCount == 0 && countChange < 0) {
            // we have just become idle
            setStatus("Idle");
        }
        if (executionCount == 1 && countChange > 0) {
            // we have just become idle
            setStatus("Working");
        }
    }
    /*
     *************************************** 
     ***************************************
     * Methods to wait for new assignments.*
     *************************************** 
     ***************************************
     */

    Watcher newTaskWatcher = new Watcher(){
        public void process(WatchedEvent e) {
            //Determines if the znode corresponding to the worker has been changed.
            //i.e. it was given a new task.
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert new String("/assign/worker-"+ serverId ).equals( e.getPath() );

                getTasks();
            }
        }
    };

    void getTasks(){
        /*
         * Gets all of the children of the /assign/worker-{serverId}. These are all
         * of the tasks that were assigned to this worker by the master. Callback is
         * called to execute the task.
         */
        zk.getChildren("/assign/worker-" + serverId,
                newTaskWatcher,
                tasksGetChildrenCallback,
                null);
    }


    protected ChildrenCache assignedTasksCache = new ChildrenCache();

    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if(children != null){
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback cb;

                            /*
                             * Initializes input of anonymous class
                             */
                            public Runnable init (List<String> children, DataCallback cb) {
                                this.children = children;
                                this.cb = cb;

                                return this;
                            }

                            public void run() {
                                if(children == null) {
                                    return;
                                }

                                LOG.info("Looping into tasks");
                                setStatus("Working");
                                for(String task : children){
                                    LOG.trace("New task: {}", task);
                                    //Reads the data at the given znode to so it knows exactly
                                    //what the task is.
                                    zk.getData("/assign/worker-" + serverId  + "/" + task,
                                            false,
                                            cb,
                                            task);
                                }
                            }
                        }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                    }
                    break;
                default:
                    System.out.println("getChildren failed: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    //Tries to get the data at the path in the event that of a connection loss.
                    zk.getData(path, false, taskDataCallback, null);
                    break;
                case OK:
                /*
                 *  Executing a task in this example is simply printing out
                 *  some string representing the task.
                 */
                    executor.execute( new Runnable() {
                        byte[] data;
                        Object ctx;

                        /*
                         * Initializes the variables this anonymous class needs
                         */
                        public Runnable init(byte[] data, Object ctx) {
                            this.data = data;
                            this.ctx = ctx;

                            return this;
                        }

                        public void run() {
                            String task = new String(data);
                            String taskNoFirst = task;
                            if(taskNoFirst.endsWith("ContainsFirstKey")){
                                taskNoFirst = taskNoFirst.substring(0, taskNoFirst.indexOf("ContainsFirstKey"));
                            }
                            LOG.info("Executing your task: " + taskNoFirst);
                            TaskType taskType = determineTask(task);
                            if (taskType == TaskType.INSERT) {
                                String[] split = task.split(" ");
                                String message = "ID: " + split[1] + " with value: " + split[2] + " has been stored in the system.";
                                executeZKCreate(ctx, message);
                            } else if (taskType == TaskType.RETRIEVE) {
                                String[] split = task.split(" ");
                                String message;

                                if(result == null){
                                    message = "ID: " + split[1] + " doesn't exist in the system.";
                                }
                                else{
                                    message = "ID: " + split[1] + " contains the value: " + result.toString();
                                }

                                executeZKCreate(ctx, message);
                                result = null;
                            } else if (taskType == TaskType.DELETE) {
                                String[] split = task.split(" ");
                                String message = "ID: " + split[1] + " has been removed from the system.";
                                executeZKCreate(ctx, message);
                            }
                            else if(taskType == TaskType.SUBMIT){
                                executeZKCreate(ctx, returnString);
                                returnString = null;
                            }
                            /*
                             * Deletes the task znode in the workers's znode parent to ensure that
                             * it won't try to execute the same task twice.
                             */
                            zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
                                    -1, taskVoidCallback, null);
                        }
                    }.init(data, ctx));

                    break;
                default:
                    LOG.error("Failed to get task data: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    private void executeZKCreate(Object ctx, String message){
        /*
         * Creates a new znode in the completed znode. The data is the completed
         * task. The master has a watcher on completed znode, so adding a new znode
         * will trigger the watcher in the master, thereby informing the master that
         * the task was completed.
         */
        zk.create("/completed/" + (String) ctx,
                (message).getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, taskStatusCreateCallback, ctx);
    }

    StringCallback taskStatusCreateCallback = new StringCallback(){
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    //Creates again in the event of a connection loss.
                    zk.create(path + "/completed", "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                            taskStatusCreateCallback, null);
                    break;
                case OK:
                    LOG.info("Created completed znode correctly: " + name);
                    break;
                case NODEEXISTS:
                    LOG.warn("Node exists: " + path);
                    break;
                default:
                    LOG.error("Failed to create task data: ", KeeperException.create(Code.get(rc), path));
            }

        }
    };

    VoidCallback taskVoidCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    break;
                case OK:
                    LOG.info("Task correctly deleted: " + path);
                    break;
                default:
                    LOG.error("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    private TaskType determineTask(String task){
        if(task.toLowerCase().startsWith("insert")){
            executeInsertRequest(task);
            return TaskType.INSERT;
        }
        else if(task.toLowerCase().startsWith("retrieve")){
            executeRetrieveRequest(task);
            return TaskType.RETRIEVE;
        }
        else if(task.toLowerCase().startsWith("delete")){
            executeDeleteRequest(task);
            return TaskType.DELETE;
        }
        else if(task.toLowerCase().startsWith("submit")){
            executeSubmitRequest(task);
            return TaskType.SUBMIT;
        }
        else{
            return TaskType.INVALID;
        }
    }

    private void executeInsertRequest(String insertRequest){
        String[] split = insertRequest.split(" ");
        BigDecimal numValue = new BigDecimal(split[2]);
        values.put(split[1], numValue);
        LOG.info("worker-" + serverId + " has stored key: " + split[1] + ", with value: " + split[2]);
    }

    private void executeRetrieveRequest(String retrieveRequest){
        LOG.info("worker-" + serverId + " retrieving key: " + retrieveRequest.split(" ")[1]);
        result = values.get(retrieveRequest.split(" ")[1]);
    }

    private void executeDeleteRequest(String deleteRequest){
        LOG.info("worker-" + serverId + " deleting key: " + deleteRequest.split(" ")[1]);
        values.remove(deleteRequest.split(" ")[1]);
    }

    private void executeSubmitRequest(String submitRequest){
        String[] split = submitRequest.split(" ");
        LOG.info("worker-" + serverId + " performing sub-calculation: " + submitRequest.substring(7));
        String op = split[1];
        boolean first = split[split.length - 1].equals("ContainsFirstKey");
        BigDecimal numToReturn = new BigDecimal(values.get(split[2]).doubleValue());
        if(!first){
            if(op.equals("/")) {
                numToReturn = new BigDecimal(1 / numToReturn.doubleValue());
            }
            else if(op.equals("-")){
                numToReturn = new BigDecimal(-1 * numToReturn.doubleValue());
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("worker-");
        sb.append(serverId);
        sb.append("~");
        sb.append(values.get(split[2]));
        sb.append(" ");
        int length = first ? split.length - 1 : split.length;
        for(int i = 3; i < length; i++){
            sb.append(values.get(split[i]));
            sb.append(" ");
            BigDecimal nextNum = new BigDecimal(values.get(split[i]).doubleValue());
            if(op.equals("+")){
                numToReturn = numToReturn.add(nextNum);
            }
            else if(op.equals("-")){
                nextNum = new BigDecimal(-1 * nextNum.doubleValue());
                numToReturn = numToReturn.add(nextNum);
            }
            else if(op.equals("*")){
                numToReturn = numToReturn.multiply(nextNum);
            }
            else if(op.equals("/")){
                nextNum = new BigDecimal(1 / nextNum.doubleValue());
                numToReturn = numToReturn.multiply(nextNum);
            }
        }
        sb.append("~");
        sb.append(numToReturn.toString());
        returnString = sb.toString();
    }

    /**
     * Closes the ZooKeeper session.
     */
    @Override
    public void close()
            throws IOException
    {
        LOG.info( "Closing" );
        try{
            //Close connection to server.
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
    }

    /**
     * Main method showing the steps to execute a worker.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        Worker w = new Worker(args[0]);
        w.startZK();

        while(!w.isConnected()){
            Thread.sleep(100);
        }   
        /*
         * bootstrap() create some necessary znodes.
         */
        w.bootstrap();
        
        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w.register();
        
        /*
         * Getting assigned tasks.
         */
        w.getTasks();

        while(!w.isExpired()){
            Thread.sleep(1000);
        }

    }

}
