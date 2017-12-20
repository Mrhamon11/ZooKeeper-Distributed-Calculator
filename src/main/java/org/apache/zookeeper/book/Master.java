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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
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
import org.apache.zookeeper.KeeperException.Code;

import org.apache.zookeeper.book.recovery.RecoveredAssignments;
import org.apache.zookeeper.book.recovery.RecoveredAssignments.RecoveryCallback;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;


/**
 * This class implements the master of the master-worker example we use
 * throughout the book. The master is responsible for tracking the list of
 * available workers, determining when there are new tasks and assigning
 * them to available workers. 
 * 
 * The flow without crashes is like this. The master reads the list of
 * available workers and watch for changes to the list of workers. It also
 * reads the list of tasks and watches for changes to the list of tasks.
 * For each new task, it assigns the task to a worker chosen at random.
 * 
 * Before exercising the role of master, this ZooKeeper client first needs
 * to elect a primary master. It does it by creating a /master znode. If
 * it succeeds, then it exercises the role of master. Otherwise, it watches
 * the /master znode, and if it goes away, it tries to elect a new primary
 * master.
 * 
 * The states of this client are three: RUNNING, ELECTED, NOTELECTED. 
 * RUNNING means that according to its view of the ZooKeeper state, there
 * is no primary master (no master has been able to acquire the /master lock).
 * If some master succeeds in creating the /master znode and this master learns
 * it, then it transitions to ELECTED if it is the primary and NOTELECTED
 * otherwise.
 *   
 * Because workers may crash, this master also needs to be able to reassign
 * tasks. When it watches for changes in the list of workers, it also 
 * receives a notification when a znode representing a worker is gone, so 
 * it is able to reassign its tasks.
 * 
 * A primary may crash too. In the case a primary crashes, the next primary
 * that takes over the role needs to make sure that it assigns and reassigns
 * tasks that the previous primary hasn't had time to process.
 *
 */
public class Master implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    
    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.  
     */
    enum MasterStates {RUNNING, ELECTED, NOTELECTED};

    private volatile MasterStates state = MasterStates.RUNNING;

    MasterStates getState() {
        return state;
    }
    
    private Random random = new Random(this.hashCode());
    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString( random.nextInt() );
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    private volatile Map<String, String> keyToWorker;
    private volatile int numResponsesBeforeSubmit;
    private volatile int numWorkerResponses;
    private volatile String op;
    private volatile String firstKey;
    private volatile List<String> workerTaskOrder;
    private volatile Map<String, String[]> workerToReturnList;
    private volatile Map<String, Integer> workerToCounter;
    private volatile int pathCounter;
    
    protected ChildrenCache tasksCache;
    protected ChildrenCache workersCache;
    
    /**
     * Creates a new master instance.
     * 
     * @param hostPort
     */
    Master(String hostPort) { 
        this.hostPort = hostPort;
        this.keyToWorker = new HashMap<>();
        this.numResponsesBeforeSubmit = 0;
        this.numWorkerResponses = 0;
        this.op = null;
        this.firstKey = null;
        this.workerTaskOrder = new ArrayList<>();
        this.workerToReturnList = new HashMap<>();
        this.workerToCounter = new HashMap<>();
        this.pathCounter = 0;
    }
    
    
    /**
     * Creates a new ZooKeeper session.
     * 
     * @throws IOException
     */
    void startZK() throws IOException {
        //Connects the master process to the zookeeper server
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    /**
     * Closes the ZooKeeper session.
     *
     * @throws IOException
     */
    void stopZK() throws InterruptedException, IOException {
        //closes the connection to the zookeeper server
        zk.close();
    }
    
    /**
     * This method implements the process method of the
     * Watcher interface. We use it to deal with the
     * different states of a session. 
     * 
     * @param e new session event to be processed
     */
    public void process(WatchedEvent e) {  
        LOG.info("Processing event: " + e.toString());
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
                LOG.error("Session expiration");
            default:
                break;
            }
        }
    }
    
    
    /**
     * This method creates some parent znodes we need for this example.
     * In the case the master is restarted, then this method does not
     * need to be executed a second time.
     */
    public void bootstrap(){
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
        createParent("/completed", new byte[0]);
    }
    
    void createParent(String path, byte[] data){
        //creates all of the top level znodes if they don't
        //already exist.
        zk.create(path, 
                data, 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                createParentCallback, 
                data);
    }
    
    StringCallback createParentCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                 */
                createParent(path, (byte[]) ctx);
                
                break;
            case OK:
                LOG.info("Parent created");
                
                break;
            case NODEEXISTS:
                LOG.warn("Parent already registered: " + path);
                
                break;
            default:
                LOG.error("Something went wrong: ", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
        
    /**
     * Check if this client is connected.
     * 
     * @return boolean ZooKeeper client is connected
     */
    boolean isConnected() {
        return connected;
    }
    
    /**
     * Check if the ZooKeeper session has expired.
     * 
     * @return boolean ZooKeeper session has expired
     */
    boolean isExpired() {
        return expired;
    }

    /*
     **************************************
     **************************************
     * Methods related to master election.*
     **************************************
     **************************************
     */
    
    
    /*
     * The story in this callback implementation is the following.
     * We tried to create the master lock znode. If it suceeds, then
     * great, it takes leadership. However, there are a couple of
     * exceptional situations we need to take care of. 
     * 
     * First, we could get a connection loss event before getting
     * an answer so we are left wondering if the operation went through.
     * To check, we try to read the /master znode. If it is there, then
     * we check if this master is the primary. If not, we run for master
     * again. 
     * 
     *  The second case is if we find that the node is already there.
     *  In this case, we call exists to set a watch on the znode.
     */
    StringCallback masterCreateCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                checkMaster();
                
                break;
            case OK:
                state = MasterStates.ELECTED;
                takeLeadership();

                break;
            case NODEEXISTS:
                state = MasterStates.NOTELECTED;
                masterExists();
                
                break;
            default:
                state = MasterStates.NOTELECTED;
                LOG.error("Something went wrong when running for master.", 
                        KeeperException.create(Code.get(rc), path));
            }
            LOG.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
        }
    };

    void masterExists() {
        /*
         * Used to determine if the "master" znode exists in the system,
         * i.e. there is already a master present. Important so that only
         * one master can function at a time.
         */
        zk.exists("/master", 
                masterExistsWatcher, 
                masterExistsCallback, 
                null);
    }
    
    StatCallback masterExistsCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                masterExists();
                
                break;
            case OK:
                break;
            case NONODE:
                state = MasterStates.RUNNING;
                runForMaster();
                LOG.info("It sounds like the previous master is gone, " +
                    		"so let's run for master again."); 
                
                break;
            default:     
                checkMaster();
                break;
            }
        }
    };
    
    Watcher masterExistsWatcher = new Watcher(){
        public void process(WatchedEvent e) {
            //If master doesn't exist anymore because it was deleted,
            //run for master again.
            if(e.getType() == EventType.NodeDeleted) {
                assert "/master".equals( e.getPath() );
                
                runForMaster();
            }
        }
    };
    
    void takeLeadership() {
        LOG.info("Going for list of workers");
        getWorkers();
        
        (new RecoveredAssignments(zk)).recover( new RecoveryCallback() {
            public void recoveryComplete(int rc, List<String> tasks) {
                if(rc == RecoveryCallback.FAILED) {
                    LOG.error("Recovery of assigned tasks failed.");
                } else {
                    LOG.info( "Assigning recovered tasks" );
                    getTasks();
                    getCompletedTasks();
                }
            }
        });
    }
    
    /*
     * Run for master. To run for master, we try to create the /master znode,
     * with masteCreateCallback being the callback implementation. 
     * In the case the create call succeeds, the client becomes the master.
     * If it receives a CONNECTIONLOSS event, then it needs to check if the 
     * znode has been created. In the case the znode exists, it needs to check
     * which server is the master.
     */
    
    /**
     * Tries to create a /master lock znode to acquire leadership.
     */
    public void runForMaster() {
        LOG.info("Running for master");
        /*
         * Creates an ephemeral master znode so that if it dies, other
         * backup masters will take over. They will know master is dead
         * because the master znode will be gone.
         */
        zk.create("/master", 
                serverId.getBytes(), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null);
    }
        
    DataCallback masterCheckCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                
                break;
            case NONODE:
                runForMaster();
                
                break; 
            case OK:
                if( serverId.equals( new String(data) ) ) {
                    state = MasterStates.ELECTED;
                    takeLeadership();
                } else {
                    state = MasterStates.NOTELECTED;
                    masterExists();
                }
                
                break;
            default:
                LOG.error("Error when reading data.", 
                        KeeperException.create(Code.get(rc), path));               
            }
        } 
    };
        
    void checkMaster() {
        /*Checks the data at the master znode. If the master process's server id
         *equals the data, it is elected master, otherwise, it is not elected.
         *This happens in the callback
         */
        zk.getData("/master", false, masterCheckCallback, null);
    }
    
    /*
     ****************************************************
     **************************************************** 
     * Methods to handle changes to the list of workers.*
     ****************************************************
     ****************************************************
     */
    
    
    /**
     * This method is here for testing purposes.
     * 
     * @return size Size of the worker list
     */
    public int getWorkersSize(){
        if(workersCache == null) {
            return 0;
        } else {
            return workersCache.getList().size();
        }
    }
    
    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/workers".equals( e.getPath() );
                
                getWorkers();
            }
        }
    };
    
    void getWorkers(){
        /*
         * Checks if workers are up/exist. Important so that it knows
         * which machine it can assign tasks to.
         */
        zk.getChildren("/workers", 
                workersChangeWatcher, 
                workersGetChildrenCallback, 
                null);
    }
    
    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                LOG.info("Succesfully got a list of workers: " 
                        + children.size() 
                        + " workers");
                reassignAndSet(children);
                break;
            default:
                LOG.error("getChildren failed",  
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     *******************
     *******************
     * Assigning tasks.*
     *******************
     *******************
     */
    
    void reassignAndSet(List<String> children){
        List<String> toProcess;
        
        if(workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info( "Removing and setting" );
            toProcess = workersCache.removedAndSet( children );
        }
        
        if(toProcess != null) {
            for(String worker : toProcess){
                getAbsentWorkerTasks(worker);
            }
        }
    }
    
    void getAbsentWorkerTasks(String worker){
        /*
         * If a worker process stopped, and it had a list of tasks that were
         * incomplete, this call allows the master to pull the data for task
         * reassignment.
         */
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }
    
    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getAbsentWorkerTasks(path);
                
                break;
            case OK:
                LOG.info("Succesfully got a list of assignments: " 
                        + children.size() 
                        + " tasks");
                
                /*
                 * Reassign the tasks of the absent worker.  
                 */
                
                for(String task: children) {
                    getDataReassign(path + "/" + task, task);                    
                }
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. * 
     ************************************************
     */
    
    /**
     * Get reassigned task data.
     * 
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
        /*
         * Gets the data at the path for task reassignment.
         */
        zk.getData(path, 
                false, 
                getDataReassignCallback, 
                task);
    }
    
    /**
     * Context for recreate operation.
     *
     */
    class RecreateTaskCtx {
        String path; 
        String task;
        byte[] data;
        
        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx); 
                
                break;
            case OK:
                recreateTask(new RecreateTaskCtx(path, (String) ctx, data));
                
                break;
            default:
                LOG.error("Something went wrong when getting data ",
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Recreate task znode in /tasks
     * 
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
        /*
         * Recreates the task znode in task so master will
         * know about the task later, and reassign it to a
         * different worker.
         */
        zk.create("/tasks/" + ctx.task,
                ctx.data,
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
    }
    
    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((RecreateTaskCtx) ctx);
       
                break;
            case OK:
                deleteAssignment(((RecreateTaskCtx) ctx).path);
                
                break;
            case NODEEXISTS:
                LOG.info("Node exists already, but if it hasn't been deleted, " +
                		"then it will eventually, so we keep trying: " + path);
                recreateTask((RecreateTaskCtx) ctx);
                
                break;
            default:
                LOG.error("Something went wrong when recreating task",
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Delete assignment of absent worker
     * 
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path){
        zk.delete(path, -1, taskDeletionCallback, null);
    }
    
    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                LOG.info("Task correctly deleted: " + path);
                break;
            default:
                LOG.error("Failed to delete task data" + 
                        KeeperException.create(Code.get(rc), path));
            } 
        }
    };
    
    /*
     ******************************************************
     ******************************************************
     * Methods for receiving new tasks and assigning them.*
     ******************************************************
     ******************************************************
     */
      
    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            //If there is a change in the tasks znode, i.e. we have a new task,
            //then we get the task and proceed to assigning them.
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/tasks".equals( e.getPath() );
                
                getTasks();
            }
        }
    };
    
    void getTasks(){
        /*
         * Gets the children of tasks znode, i.e. the actual tasks with their requests.
         * The tasksGetChildrenCallback is activated and all of the tasks are then
         * assigned to workers.
         */
        zk.getChildren("/tasks", 
                tasksChangeWatcher, 
                tasksGetChildrenCallback, 
                null);
    }
    
    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                
                break;
            case OK:
                List<String> toProcess;
                if(tasksCache == null) {
                    tasksCache = new ChildrenCache(children);
                    
                    toProcess = children;
                } else {
                    toProcess = tasksCache.addedAndSet( children );
                }
                
                if(toProcess != null){
                    assignTasks(toProcess);
                } 
                
                break;
            default:
                LOG.error("getChildren failed.",  
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    void assignTasks(List<String> tasks) {
        for(String task : tasks){
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
        /*
         * Get the data, i.e. the actual request from the client, from the znode,
         * and taskDataCallback is executed which will determine which kind of
         * request it is.
         */
        zk.getData("/tasks/" + task, 
                false, 
                taskDataCallback, 
                task);
    }
    
    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTaskData((String) ctx);
                
                break;
            case OK:
                LOG.info("Received new task from client: " + new String(data));
                determineTask(ctx, data);
                break;
            default:
                LOG.error("Error when trying to get task data.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    private void determineTask(Object ctx, byte[] data) {
        String task = new String(data);
        String trimmedTask = task.trim().toLowerCase();
        if (trimmedTask.startsWith("insert")) {
            insertTask(ctx, task);
        }
        else if(trimmedTask.startsWith("retrieve") || task.startsWith("delete")){
            retrieveOrDeleteTask(ctx, task);
        }
        else if(trimmedTask.startsWith("submit")){
            submitTask(ctx, task);
        }
    }

    private void insertTask(Object ctx, String data){


        /*
         * If this key has already been assigned to a worker on a previous run, assign
         * task to that worker to update value.
         * Otherwise, choose worker at random.
         */
        String designatedWorker;

        if(keyToWorker.containsKey(data.split(" ")[1])){
            designatedWorker = keyToWorker.get(data.split(" ")[1]);

        }
        else{
            List<String> list = workersCache.getList();
            designatedWorker = list.get(random.nextInt(list.size()));
        }

        LOG.info("Assigning insert task to " + designatedWorker + " Task: " +
                data);

        assign(ctx, data, designatedWorker);

        keyToWorker.put(data.split(" ")[1], designatedWorker);
//        LOG.info(new String(data) + " " + keyToWorker.get(new String(data)));
    }

    private void retrieveOrDeleteTask(Object ctx, String data){
        String key = data.split(" ")[1];
        String worker = keyToWorker.get(key);
        if(worker == null){
            LOG.error("ID " + key + " has never been entered into the system with a corresponding value!");
            return;
        }
        //If delete, remove from master database
        if(data.startsWith("delete")){
            keyToWorker.remove(key);
            LOG.info("Assigning delete task to " + worker + " Task: " +
                    data);
        }
        else {
            LOG.info("Assigning retrieve task to " + worker + " Task: " +
                    data);
        }
        assign(ctx, data, worker);
    }

    private void submitTask(Object ctx, String data){
        String[] taskSplit = data.split(" ");
        if(!allKeysStoredInWorkers(taskSplit)){
            LOG.error("All IDs must be stored in system before any calculations involving them can be run!");
            return;
        }

        LOG.info("Breaking up submit task into separate parts, and sending to workers." +
                " Task: " + data);

        determineNumResponses(taskSplit);

        op = taskSplit[1];
        firstKey = taskSplit[2];

        setUpDataStructs(taskSplit);

        Map<String, StringBuilder> sbs = new HashMap<>();
        String workerWithFirstKey = "";
        for(int i = 2; i < taskSplit.length; i++){
            String worker = keyToWorker.get(taskSplit[i]);
            if(!sbs.containsKey(worker)){
                StringBuilder sb = new StringBuilder();
                sb.append("submit ");
                sb.append(op);
                sb.append(" ");
                sbs.put(worker, sb);
            }
            sbs.get(worker).append(taskSplit[i]);
            sbs.get(worker).append(" ");
            if(i == 2){
                workerWithFirstKey = worker;
            }
        }
        sbs.get(workerWithFirstKey).append("ContainsFirstKey");

        for(String worker : sbs.keySet()){
            String toLog = sbs.get(worker).toString();
            if(toLog.endsWith("ContainsFirstKey")){
                toLog = toLog.substring(0, toLog.indexOf("ContainsFirstKey"));
            }
            LOG.info("Assigning correct submit task to " + worker + " Task: " + toLog);
            assign(ctx, sbs.get(worker).toString(), worker);
        }
    }

    private boolean allKeysStoredInWorkers(String[] taskSplit){
        for(int i = 2; i < taskSplit.length; i++){
            if(keyToWorker.get(taskSplit[i]) == null)
                return false;
        }
        return true;
    }

    private void determineNumResponses(String[] taskSplit){
        Set<String> numWorkersContainingIDs = new HashSet<>();
        for(int i = 2; i < taskSplit.length; i++){
            String worker = keyToWorker.get(taskSplit[i]);
            if(!numWorkersContainingIDs.contains(worker)){
                numWorkersContainingIDs.add(worker);
                numResponsesBeforeSubmit++;
            }
        }
    }

    private void setUpDataStructs(String[] taskSplit){
        for(int i = 2; i < taskSplit.length; i++){
            String worker = keyToWorker.get(taskSplit[i]);
            workerTaskOrder.add(worker);
            workerToReturnList.put(worker, null);
            workerToCounter.put(worker, 0);
        }
    }

    private void assign(Object ctx, String data, String designatedWorker){
        /*
         * Assign task supplied worker.
         */
        String assignmentPath = "/assign/" +
                designatedWorker +
                "/" +
                (String) ctx + "-" + pathCounter;
        pathCounter++;
        if(pathCounter == numResponsesBeforeSubmit) {
            pathCounter = 0;
        }
        LOG.info( "Assignment path: " + assignmentPath );
        createAssignment(assignmentPath, data.getBytes());
    }
    
    void createAssignment(String path, byte[] data){
        /*
         * Creates the assignment to a specific machine. The path is a path
         * to a specific worker, and is placed in the assign znode. The
         * worker chosen will have a watcher on this path as it contains
         * the name of the worker.
         */
        zk.create(path, 
                data, 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                assignTaskCallback, 
                data);
    }
    
    StringCallback assignTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) { 
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);
                
                break;
            case OK:
                LOG.info("Task assigned correctly: " + name);
                deleteTask(name.substring( name.lastIndexOf("/") + 1, name.lastIndexOf("-")));
                
                break;
            case NODEEXISTS: 
                LOG.warn("Task already assigned");
                
                break;
            default:
                LOG.error("Error when trying to assign task.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    /*
     * Once assigned, we delete the task from /tasks
     */
    void deleteTask(String name){
        /*
         * Deletes the task from the task znode so that it doesn't get assigned again.
         * Only master sees new nodes in the tasks znode, so it has to know when a task
         * has been assigned or not. Easy if it's not there anymore.
         */
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    }
    
    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteTask(path);
                
                break;
            case OK:
                LOG.info("Successfully deleted " + path);
                
                break;
            case NONODE:
                LOG.info("Task " + path + " has been deleted already");
                
                break;
            default:
                LOG.error("Something went wrong here, " + 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /*
     ******************************************************
     ******************************************************
     * Methods for receiving completed tasks and sending them back to client.*
     ******************************************************
     ******************************************************
     */

    Watcher tasksCompletedWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            /*
             * If there is a change in the completed znode, it means that a
             * worker has finished a task. If completed has a child, get the
             * data from the completed task and return back to client, doing any
             * other necessary computations if necessary.
             */
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/completed".equals( e.getPath() );

                getCompletedTasks();
            }
        }
    };

    void getCompletedTasks(){
        /*
         * Gets the children of completed znode, i.e. the actual tasks with their requests.
         * The completedTasksGetChildrenCallback is activated and all of the completed
         * tasks are then sent back to clients.
         */
        zk.getChildren("/completed",
                tasksCompletedWatcher,
                completedTasksGetChildrenCallback,
                null);
    }

    ChildrenCallback completedTasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    getCompletedTasks();

                    break;
                case OK:
                    List<String> toProcess;
                    if(tasksCache == null) {
                        tasksCache = new ChildrenCache(children);

                        toProcess = children;
                    } else {
                        toProcess = tasksCache.addedAndSet( children );
                    }


                    if(toProcess != null){
                        handleCompletedTasks(toProcess);
                    }

                    break;
                default:
                    LOG.error("getChildren failed.",
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void handleCompletedTasks(List<String> tasks) {
//        Map<String, List<String>> taskGroups = splitIntoParts(tasks);
//        for(String taskGroup : taskGroups.keySet()){
//            List<String> groupedTasks = taskGroups.get(taskGroup);
//            for(String task : groupedTasks){
//                getCompletedTaskData(task);
//            }
//        }
        for(String task : tasks){
            getCompletedTaskData(task);
        }
    }

//    private Map<String, List<String>> splitIntoParts(List<String> tasks){
//        Map<String, List<String>> taskGroups = new HashMap<>();
//        for(String task : tasks){
//            String pref = task.substring(0, task.lastIndexOf("-"));
//            List<String> group = taskGroups.get(pref);
//            if(group == null){
//                group = new ArrayList<>();
//            }
//            group.add(task);
//            taskGroups.put(pref, group);
//        }
//        return taskGroups;
//    }

    void getCompletedTaskData(String task) {
        /*
         * Get the data, i.e. the data passed back from the worker, and call the callback.
         * The callback will determine if the master needs to do any additional computations
         * or if it can send it back to the client immediately.
         */
        zk.getData("/completed/" + task,
                false,
                completedTaskDataCallback,
                task);
        /*
         * Deletes the znode for the completed task from zookeeper to ensure that it doesn't
         * send it to the client twice.
         */
        zk.delete("/completed/" + task, -1, completedTaskDeleteCallback, null);
    }

    DataCallback completedTaskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    getCompletedTaskData((String) ctx);

                    break;
                case OK:
                    determineCompletedTask(ctx, data);

                    break;
                default:
                    LOG.error("Error when trying to get completed task data.",
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    private void determineCompletedTask(Object ctx, byte[] data) {
        String str = new String(data);
        //Submit calculation requests will be returned back to the master from the
        //worker with different parts, separated by ~ symbols as decimeters.
        if(str.contains("~")){
            updateWorkerData(str);
            if(++numWorkerResponses == numResponsesBeforeSubmit){
                String calcs = sendCompleteCalc();
                sendToClient(ctx, calcs.getBytes());
            }
        }
        else{
            sendToClient(ctx, data);
        }
    }

    private void sendToClient(Object ctx, byte[] data){
        String path = (String) ctx;
        path = path.substring(0, path.lastIndexOf("-"));
        /*
         * Send completed task back to client
         */
        String pathToClient = "/status/" + path;

        LOG.info( "Client path: " + pathToClient );
        createSubmissionToClient(pathToClient, data);

        /*
         * Deletes the znode for the completed task from zookeeper to ensure that it doesn't
         * send it to the client twice.
         */
        zk.delete("/completed/" + (String) ctx, -1, completedTaskDeleteCallback, null);
    }



    private void updateWorkerData(String completedTask){
        String[] split = completedTask.split("~");
        String worker = split[0];
        workerToReturnList.put(worker, split);
    }

    private String sendCompleteCalc(){
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < workerTaskOrder.size(); i++){
            String worker = workerTaskOrder.get(i);
            String[] list = workerToReturnList.get(worker);
            int index = workerToCounter.get(worker);
            sb.append(list[1].split(" ")[index]);
            workerToCounter.put(worker, ++index);
            sb.append(" ");
            sb.append(op);
            sb.append(" ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("= ");
        BigDecimal num;
        if(op.equals("+") || op.equals("-")){
            num = new BigDecimal(0);
        }
        else{
            num = new BigDecimal(1);
        }
        for(String worker : workerToReturnList.keySet()){
            BigDecimal nextNum = new BigDecimal(workerToReturnList.get(worker)[2]);
            if(op.equals("+") || op.equals("-")){
                num = num.add(nextNum);
            }
            else{
                num = num.multiply(nextNum);
            }
        }
        sb.append(num.toString());
        reset();
        return sb.toString();
    }

    private void reset(){
        numResponsesBeforeSubmit = 0;
        numWorkerResponses = 0;
        workerToReturnList = new HashMap<>();
        workerTaskOrder = new ArrayList<>();
        workerToCounter = new HashMap<>();
        pathCounter = 0;
        op = null;
        pathCounter = 0;
    }

    void createSubmissionToClient(String path, byte[] data){
        /*
         * Creates a new znode in the status znode. The client will have a watcher on
         * any changes to the status znode, and so, passing this data through the status
         * znode ensures that the client will get the completed request.
         */
        zk.create(path,
                data,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                submissionToClientCallback,
                data);
    }

    StringCallback submissionToClientCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    createAssignment(path, (byte[]) ctx);

                    break;
                case OK:
                    LOG.info("Task sent back to client correctly: " + name);
                    deleteCompletedTask(name.substring( name.lastIndexOf("/") + 1));

                    break;
                case NODEEXISTS:
                    LOG.warn("Task " + path + " already returned to client.");

                    break;
                default:
                    LOG.error("Error when trying to assign task.",
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /*
     * Once assigned, we delete the task from /tasks
     */
    void deleteCompletedTask(String name){
        /*
         * Deletes the znode for the completed task from zookeeper to ensure that it doesn't
         * send it to the client twice.
         */
        zk.delete("/completed/" + name, -1, completedTaskDeleteCallback, null);
    }

    VoidCallback completedTaskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteCompletedTask(path);

                    break;
                case OK:
                    LOG.info("Successfully deleted " + path);

                    break;
                case NONODE:
                    LOG.info("Completed task " + path + " has been deleted already");

                    break;
                default:
                    LOG.error("Something went wrong here, " +
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /**
     * Closes the ZooKeeper session. 
     * 
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if(zk != null) {
            try{
                //Closes connection to zookeeper server.
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn( "Interrupted while closing ZooKeeper session.", e );
            }
        }
    }
    
    /**
     * Main method providing an example of how to run the master.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception { 
        Master m = new Master(args[0]);
        m.startZK();
        
        while(!m.isConnected()){
            Thread.sleep(100);
        }
        /*
         * bootstrap() creates some necessary znodes.
         */
        m.bootstrap();
        
        /*
         * now runs for master.
         */
        m.runForMaster();
        
        while(!m.isExpired()){
            Thread.sleep(1000);
        }   

        m.stopZK();
    }    
}
