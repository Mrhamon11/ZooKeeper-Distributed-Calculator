<h1>Distributed Calculator Using ZooKeeper</h1>

<h2>Description:</h2>
This project is a calculator application that runs in a distributed environment.
Users can store key value pairs into the system, where the value is a number,
and the key is a unique ID representing it in the system. Users can then retrieve
these numbers, or delete them from the system by supplying the ID of the number.

A user can also submit calculation requests. Users can supply an operator 
and a list of already stored keys, and the system will calculate the result.  


<h2>ZooKeeper:</h2>
This project uses ZooKeeper in order to make it distributed. This project has
three main classes: 
<ul>
    <li>Master</li>
    <li>Worker</li>
    <li>Client</li>
</ul>
The client process reads in a list of commands from the input file, and then
sends each request, one at a time, to the Master using a ZooKeeper ZNode. 
Master sends the task to a worker to complete, and Worker sends it back to
Master. If there the task requires the Master to complete it, i.e. in the case 
of a Submit request that was split up amongst multiple Workers, the Master 
must perform the final calculations, the Master will finish the job. Then it 
will send the completed request back to the Client. The transfer to task 
information takes place through ZNodes.


<h2>Requirements:</h2>

In order to use this project, a user must supply a txt file containing any 
tasks he or she wants the system to perform. All requests must be on separate
lines. User must be using a unix system to run this project.

<h2>Format for input file:</h2>
Input file containing the commands must be passed into the bash shell
script in order to run the commands. There are four kinds of requests:
<ul>
    <li>Insert</li>
    <li>Retrieve</li>
    <li>Delete</li>
    <li>Submit</li>
</ul>

An Insert request must start with the word "insert", followed by the name 
of the ID the user wants to store, followed by the numerical value. For 
example 
```
insert keyOne 1
```

A Retrieve request must start with the word "retrieve", followed by 
the name of the ID whose value is desired. For example:

```
retrieve keyOne
```

A Delete request must start with the word "delete", followed by the 
name of the ID the user wants to delete. For example:

```
delete keyOne
```

Finally, a Submit request allows the user to submit calculations. It
must start with the word "submit", followed by the operator, (choice
of +, -, *, /), and a list of keys the user wants to include.
For example:

```
submit + keyOne keyTwo keyThree
```
This will add the values in of keyOne, keyTwo, and keyThree.
Only one operator is allowed for any calculation.

Of course, keys must be inserted into the system before they can
be retrieved, deleted, or used in calculations.

<h2>How to run the project.</h2>
Once the user has created an input file only one bash script is 
required to run this project. That is the amon.sh shell script.

This script requires a three command line arguments. They are:
<ul>
    <li>The IP address and port</li>
    <li>The number of workers desired</li>
    <li>The path to the input file</li>
</ul>

By default, the ZooKeeper system is running on localhost:2181. To change,
Go into the zookeeper-3.4.11/conf/zoo.cfg file and edit the client port.

Example of running script:
```
./amon.sh localhost:2181 4 inputFile.txt
```
This project comes with a sample input file in the same directory as the 
bash script. It is called inputFile.txt.

They system will log a whole bunch of things, detailing the passing of
information between the system. When the client finishes running, the master,
all workers, and the ZooKeeper server will remain active. A user can run 
the same bash script with the command line arguments again, and it will 
function correctly. 

To close all systems, either kill each java process individually, or 
type <code>killall java</code> into the terminal. The ZooKeeper server can
also be shut down by typing in <code>zookeeper-3.4.11/bin/zkServer.sh stop</code>
in the terminal.

For a fresh run, all ZNodes that were created should be deleted. To do this,
type <code>zookeeper-3.4.11/bin/zkServer.sh star</code>in the terminal, and then
<code>zookeeper-3.4.11/bin/zkCli.sh</code> Entering <code>ls </code> should
show all existing ZNodes. To remove them, enter <code>rmr /{znodename}</code>
So to delete the "completed" ZNode, enter <code>rmr /completed</code>.
All ZNodes except for the "zookeeper" and "master" ZNodes should be deleted.
The "master" ZNode will auto delete itself. Afterwards, stop the zkServer as
described above.

A fresh run is recommended every time, although it should work without one.

<h1>Enjoy!</h1>