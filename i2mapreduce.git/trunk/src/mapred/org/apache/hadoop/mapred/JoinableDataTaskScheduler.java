package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.util.ReflectionUtils;

public class JoinableDataTaskScheduler extends TaskScheduler {
	  private static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;
	  public static final Log LOG = LogFactory.getLog(JoinableDataTaskScheduler.class);
	  
	  protected JobQueueJobInProgressListener jobQueueJobInProgressListener;
	  private EagerTaskInitializationListener eagerTaskInitializationListener;
	  private float padFraction;
	  
	  //<iteration id: tasktracker id <-> map task id> for scheduling
	  public Map<JobID, Map<String, LinkedList<Integer>>> scheduled_tracker_mtask_map = new HashMap<JobID, Map<String, LinkedList<Integer>>>();
	  
	  //<iteration id: tasktracker id <-> reduce task id> for scheduling
	  public Map<JobID, Map<String, LinkedList<Integer>>> scheduled_tracker_rtask_map = new HashMap<JobID, Map<String, LinkedList<Integer>>>();

	  public Map<JobID, Map<String, LinkedList<Integer>>> fail_tracker_rtask_map = new HashMap<JobID, Map<String, LinkedList<Integer>>>();
	  //<iteration id: tasktracker id <-> map task id> for tracking
	  //public Map<JobID, Map<String, ArrayList<Integer>>> tracker_mtask_map = new HashMap<JobID, Map<String, ArrayList<Integer>>>();
	  
	  //<iteration id: tasktracker id <-> reduce task id> for tracking
	  //public Map<JobID, Map<String, ArrayList<Integer>>> tracker_rtask_map = new HashMap<JobID, Map<String, ArrayList<Integer>>>();

	  //one-to-mul mapping list
	  class Assignment{
		  ArrayList<Integer> maps;
		  ArrayList<Integer> reduces;
	  }
	  
	  public Map<String, Map<String, Assignment>> one2mulAssignMap = new HashMap<String, Map<String, Assignment>>();
	  
	  //<iteration id: job list>
	  //public Map<String, HashSet<JobID>> iteration_jobs_map = new HashMap<String, HashSet<JobID>>();
	  
	  public JoinableDataTaskScheduler() {
	    this.jobQueueJobInProgressListener = new JobQueueJobInProgressListener();
	  }
	  
	  @Override
	  public synchronized void start() throws IOException {
		    super.start();
		    taskTrackerManager.addJobInProgressListener(jobQueueJobInProgressListener);
		    eagerTaskInitializationListener.setTaskTrackerManager(taskTrackerManager);
		    eagerTaskInitializationListener.start();
		    taskTrackerManager.addJobInProgressListener(
		        eagerTaskInitializationListener);
	  }
	  
	  @Override
	  public synchronized void terminate() throws IOException {
		    if (jobQueueJobInProgressListener != null) {
		        taskTrackerManager.removeJobInProgressListener(
		            jobQueueJobInProgressListener);
		      }
		      if (eagerTaskInitializationListener != null) {
		        taskTrackerManager.removeJobInProgressListener(
		            eagerTaskInitializationListener);
		        eagerTaskInitializationListener.terminate();
		      }
		      super.terminate();
	  }
	  
	  @Override
	  public synchronized void setConf(Configuration conf) {
	    super.setConf(conf);
	    padFraction = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
	                                 0.01f);
	    this.eagerTaskInitializationListener =
	    	      new EagerTaskInitializationListener(conf);
	  }

	  @Override
	  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
	      throws IOException {
		  
	    TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus(); 
	    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
	    final int numTaskTrackers = clusterStatus.getTaskTrackers();
	    final int clusterMapCapacity = clusterStatus.getMaxMapTasks();
	    final int clusterReduceCapacity = clusterStatus.getMaxReduceTasks();

	    Collection<JobInProgress> jobQueue =
	      jobQueueJobInProgressListener.getJobQueue();

	    //
	    // Get map + reduce counts for the current tracker.
	    //
	    final int trackerMapCapacity = taskTrackerStatus.getMaxMapSlots();
	    final int trackerReduceCapacity = taskTrackerStatus.getMaxReduceSlots();
	    final int trackerRunningMaps = taskTrackerStatus.countMapTasks();
	    final int trackerRunningReduces = taskTrackerStatus.countReduceTasks();

	    // Assigned tasks
	    List<Task> assignedTasks = new ArrayList<Task>();

	    //
	    // Compute (running + pending) map and reduce task numbers across pool
	    //
	    int remainingReduceLoad = 0;
	    int remainingMapLoad = 0;
	    synchronized (jobQueue) {
	      for (JobInProgress job : jobQueue) {
	        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
	          remainingMapLoad += (job.desiredMaps() - job.finishedMaps());
	          if (job.scheduleReduces()) {	
	            remainingReduceLoad += 
	              (job.desiredReduces() - job.finishedReduces());
	          }
	        }
	      }
	    }

	    // Compute the 'load factor' for maps and reduces
	    double mapLoadFactor = 0.0;
	    if (clusterMapCapacity > 0) {
	      mapLoadFactor = (double)remainingMapLoad / clusterMapCapacity;
	    }
	    double reduceLoadFactor = 0.0;
	    if (clusterReduceCapacity > 0) {
	      reduceLoadFactor = (double)remainingReduceLoad / clusterReduceCapacity;
	    }
	        
	    //
	    // In the below steps, we allocate first map tasks (if appropriate),
	    // and then reduce tasks if appropriate.  We go through all jobs
	    // in order of job arrival; jobs only get serviced if their 
	    // predecessors are serviced, too.
	    //

	    //
	    // We assign tasks to the current taskTracker if the given machine 
	    // has a workload that's less than the maximum load of that kind of
	    // task.
	    // However, if the cluster is close to getting loaded i.e. we don't
	    // have enough _padding_ for speculative executions etc., we only 
	    // schedule the "highest priority" task i.e. the task from the job 
	    // with the highest priority.
	    //
	    
	    final int trackerCurrentMapCapacity = 
	      Math.min((int)Math.ceil(mapLoadFactor * trackerMapCapacity), 
	                              trackerMapCapacity);
	    int availableMapSlots = trackerCurrentMapCapacity - trackerRunningMaps;
	    boolean exceededMapPadding = false;
	    if (availableMapSlots > 0) {
	      exceededMapPadding = 
	        exceededPadding(true, clusterStatus, trackerMapCapacity);
	    }
	    
	    int numLocalMaps = 0;
	    int numNonLocalMaps = 0;
	    scheduleMaps:
	    for (int i=0; i < availableMapSlots; ++i) {
	      synchronized (jobQueue) {
	        for (JobInProgress job : jobQueue) {
	          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
	            continue;
	          }

	          if(job.getJobConf().isIterative() || job.getJobConf().isIterCPC()){
	        	  int res = assignMapForIterative(taskTracker, job, numTaskTrackers, taskTrackerStatus, 
	        			  exceededMapPadding, assignedTasks);
	        	  if(res == -1){
	        		  break;
	        	  }else if(res == 1){
	        		  break scheduleMaps;
	        	  }
	          }else if(job.getJobConf().isIncrementalStart() || 
	        		  job.getJobConf().isIncrementalIterative() ||
	        		  job.getJobConf().isIncrMRBGOnly()){
	        	  int res = assignMapForIterative(taskTracker, job, numTaskTrackers, taskTrackerStatus, 
	        			  exceededMapPadding, assignedTasks);
	        	  if(res == -1){
	        		  break;
	        	  }else if(res == 1){
	        		  break scheduleMaps;
	        	  }
	          }else{
	        	//not an iterative or incremental algorithm, normal schedule
	        	  Task t = null;
	              
	              // Try to schedule a node-local or rack-local Map task
	              t = 
	                job.obtainNewNodeOrRackLocalMapTask(taskTrackerStatus, 
	                    numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts());
	              if (t != null) {
	                assignedTasks.add(t);
	                ++numLocalMaps;
	                
	                // Don't assign map tasks to the hilt!
	                // Leave some free slots in the cluster for future task-failures,
	                // speculative tasks etc. beyond the highest priority job
	                if (exceededMapPadding) {
	                  break scheduleMaps;
	                }
	               
	                // Try all jobs again for the next Map task 
	                break;
	              }
	              
	              // Try to schedule a node-local or rack-local Map task
	              t = 
	                job.obtainNewNonLocalMapTask(taskTrackerStatus, numTaskTrackers,
	                                       taskTrackerManager.getNumberOfUniqueHosts());
	              
	              if (t != null) {
	                assignedTasks.add(t);
	                ++numNonLocalMaps;
	                
	                // We assign at most 1 off-switch or speculative task
	                // This is to prevent TaskTrackers from stealing local-tasks
	                // from other TaskTrackers.
	                break scheduleMaps;
	              }
	          }
	        }
	      }
	    }
	    int assignedMaps = assignedTasks.size();

	    //
	    // Same thing, but for reduce tasks
	    // However we _never_ assign more than 1 reduce task per heartbeat
	    //
	    /**
	     * should maintain the reduce task location for the termination check
	     */
	    final int trackerCurrentReduceCapacity = 
	      Math.min((int)Math.ceil(reduceLoadFactor * trackerReduceCapacity), 
	               trackerReduceCapacity);
	    final int availableReduceSlots = 
	      Math.min((trackerCurrentReduceCapacity - trackerRunningReduces), 1);
	    boolean exceededReducePadding = false;
	    /*
	    LOG.info("availableReduceSlots " + availableReduceSlots + 
	    		"\ttrackerCurrentReduceCapacity=" + trackerCurrentReduceCapacity +
	    		"\ttrackerRunningReduces=" + trackerRunningReduces +
	    		"\treduceLoadFactor=" + reduceLoadFactor + 
	    		"\ttrackerReduceCapacity=" + trackerReduceCapacity);
	    		*/
	    if (availableReduceSlots > 0) {
	    	
	      exceededReducePadding = exceededPadding(false, clusterStatus, 
	                                              trackerReduceCapacity);
	      synchronized (jobQueue) {
	        for (JobInProgress job : jobQueue) {
	        	LOG.info("job " + job.getJobID() + " assigning reduce tasks!");
	          if (job.getStatus().getRunState() != JobStatus.RUNNING ||
	              job.numReduceTasks == 0) {
	        	  LOG.info("have to continue " + job.getStatus().getRunState());
	            continue;
	          }

	          Task t = null;

	          if(job.getJobConf().isIterative() || job.getJobConf().isIncrementalIterative()
	        		  || job.getJobConf().isIncrMRBGOnly() || job.getJobConf().isIterCPC()){
	        	  int res = assignReduceForIterative(taskTracker, job, numTaskTrackers, taskTrackerStatus, 
	        			  exceededMapPadding, assignedTasks);
	        	  if(res == -1) break;
	          }else{
	        	  t = job.obtainNewReduceTask(taskTrackerStatus, numTaskTrackers, 
                          taskTrackerManager.getNumberOfUniqueHosts());
	        	  
		          if (t != null) {
			            assignedTasks.add(t);
			            break;
			      }
	          }
	        }
	      }
	    }
	    
	    if (LOG.isDebugEnabled()) {
	      LOG.debug("Task assignments for " + taskTrackerStatus.getTrackerName() + " --> " +
	                "[" + mapLoadFactor + ", " + trackerMapCapacity + ", " + 
	                trackerCurrentMapCapacity + ", " + trackerRunningMaps + "] -> [" + 
	                (trackerCurrentMapCapacity - trackerRunningMaps) + ", " +
	                assignedMaps + " (" + numLocalMaps + ", " + numNonLocalMaps + 
	                ")] [" + reduceLoadFactor + ", " + trackerReduceCapacity + ", " + 
	                trackerCurrentReduceCapacity + "," + trackerRunningReduces + 
	                "] -> [" + (trackerCurrentReduceCapacity - trackerRunningReduces) + 
	                ", " + (assignedTasks.size()-assignedMaps) + "]");
	    }

	    return assignedTasks;
	  }

	  private int assignMapForIterative(TaskTracker taskTracker, JobInProgress job,
			  int numTaskTrackers, TaskTrackerStatus taskTrackerStatus, boolean exceededMapPadding, 
			  List<Task> assignedTasks) throws IOException{
		  Task t = null;
		  JobID jobid = job.getJobID();
    	  String jointype = job.getJobConf().get("mapred.iterative.jointype");
    	  
    	  //prepare the tracker->maptasklist hashmap
          if(!this.scheduled_tracker_mtask_map.containsKey(jobid)){
        	  Map<String, LinkedList<Integer>> new_tracker_mtask_map = new HashMap<String, LinkedList<Integer>>();
        	  this.scheduled_tracker_mtask_map.put(jobid, new_tracker_mtask_map);
        	  
        	  Map<String, LinkedList<Integer>> new_scheduled_tracker_rtask_map = new HashMap<String, LinkedList<Integer>>();
        	  this.scheduled_tracker_rtask_map.put(jobid, new_scheduled_tracker_rtask_map);
          }

    	  /**
    	   * the one2mul case should be carefully taken care, we want to assgin map0,map1,map2 and reduce0 to a tracker,
    	   * and assign map3,map4,map5 and reduce1 to another tracker
    	   */
          String trackername = taskTracker.getTrackerName();
    	  LOG.info("jointype is " + jointype + " for tracker " + trackername);
    	  
    	  if(jointype.equals("one2mul")){
    		  
    		  /**
    		   * if contain the tracker, that means we have assigned tasks for this tracker,
    		   * so we only consider the not-contained case
    		   */
	          if(!this.scheduled_tracker_mtask_map.get(jobid).containsKey(trackername)){
	        	  LinkedList<Integer> tasklist = new LinkedList<Integer>();
	        	  this.scheduled_tracker_mtask_map.get(jobid).put(trackername, tasklist);
	        	  
		          if(!this.scheduled_tracker_rtask_map.get(jobid).containsKey(trackername)){
		        	  LinkedList<Integer> tasklist2 = new LinkedList<Integer>();
		        	  this.scheduled_tracker_rtask_map.get(jobid).put(trackername, tasklist2);
		          }
		          
			      //for debugging
				  String debugout1 = "maps: ";
				  String debugout2 = "reduces: ";
				  
	    		  int scala = job.getJobConf().getInt("mapred.iterative.data.scala", 1);
				  int reducersEachTracker = (int) Math.ceil(((double)job.getJobConf().getNumReduceTasks()) / numTaskTrackers);
				  int reduceOffsetId = (scheduled_tracker_rtask_map.get(jobid).size()-1) * reducersEachTracker; //the start reduce id
				  int mappers = job.getJobConf().getNumMapTasks();
				  int reducers = job.getJobConf().getNumReduceTasks();
				  if(mappers%reducers != 0) throw new IOException("mappers " + mappers + " reducers " + reducers + " can not be % to 0!");
				  
				  //reducer-tracker 0,1,2->0; 3,4,5->1;
				  for(int count = 0; count < reducersEachTracker; count++){
					  int reducerID = reduceOffsetId + count;
					  
					  //if all reduce tasks have been assigned, reducerID is increasing
					  if(reducerID >= job.getJobConf().getNumReduceTasks()){
						  break;
					  }
						  
					  debugout2 += reducerID + " ";
					  scheduled_tracker_rtask_map.get(jobid).get(trackername).add(reducerID);
					  
					  for(int count2=0; count2<scala; count2++){
						//mapper->reducer 0,2,4->0; 1,3,5->1;
						  
						  int mapperID = count2 * reducers + reducerID;
						  //int mapid = job.splitTaskMap.get(mappartitionid);
						  debugout1 += mapperID + " ";
						  this.scheduled_tracker_mtask_map.get(jobid).get(trackername).add(mapperID);
					  }
				  }
				  
				  //print out for debug
				  LOG.info("tracker " + taskTracker.getTrackerName() + " assigned tasks:\n" +
				  		debugout1 + "\n" + debugout2);
	          }

			  //assign a map task for this tracker
        	  Integer target = null;
        	  try{
        		  target = scheduled_tracker_mtask_map.get(jobid).get(trackername).peekFirst();
        	  }catch (Exception e){
        		  e.printStackTrace();
        	  }
        	  
        	  if(target == null){
        		  //all have been assigned, no more work, maybe it should help others to process
        		  LOG.info("all map tasks on tasktracker " + trackername + " have been processed");
        		  return -1;
        	  }else{
		          t = job.obtainNewNodeOrRackLocalMapTask(taskTrackerStatus, 
			                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts(), target);
        	  }
        	  
    	  }else{
    		  //for one-to-one case and one-to-all case, make the map to store the map/reduce task assignment
	          if(!this.scheduled_tracker_mtask_map.get(jobid).containsKey(trackername)){
	        	  LinkedList<Integer> tasklist = new LinkedList<Integer>();
	        	  this.scheduled_tracker_mtask_map.get(jobid).put(trackername, tasklist);
	          }
	          
	          if(!this.scheduled_tracker_rtask_map.get(jobid).containsKey(trackername)){
	        	  LinkedList<Integer> tasklist2 = new LinkedList<Integer>();
	        	  this.scheduled_tracker_rtask_map.get(jobid).put(trackername, tasklist2);
	          }
	          
    		  t = job.obtainNewNodeOrRackLocalMapTask(taskTrackerStatus, 
		                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts()); 
    		  
    		  
    		  
    		  if(t == null) {
    			  LOG.info("no task has been assigned from " + taskTrackerStatus.getHost());
    		  }
    	  }
/*        	  
          else{
        	  Integer target = null;
        	  try{
        		  target = this.mtask_assign_map.get(job.getJobID()).get(taskTracker.getTrackerName()).peekFirst();
        	  }catch (Exception e){
        		  e.printStackTrace();
        	  }
        	  
        	  if(target == null){
        		  //all have been assigned, no more work, maybe it should help others to process
        		  LOG.info("all map tasks on tasktracker " + taskTracker.getTrackerName() + " have been processed");
        		  return -1;
        	  }else{
		          t = job.obtainNewNodeOrRackLocalMapTask(taskTrackerStatus, 
			                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts(), target);
        	  }
          }
  */        

          if (t != null) {
            assignedTasks.add(t);
            
            //new iteration job and the first task for a tasktracker
            //for one2mul case, we don't need to record the assignment, since we already made the assignment list beforehand
            if(jointype.equals("one2mul")){
            	//poll, remove
            	scheduled_tracker_mtask_map.get(jobid).get(trackername).pollFirst();
/*            	
            	//record the assignment list for map tasks
	            if(!tracker_mtask_map.get(jobid).containsKey(trackername)){
	            	LinkedList<Integer> tasklist = new LinkedList<Integer>();
	            	tracker_mtask_map.get(jobid).put(trackername, tasklist);
	            }
	            
	            tracker_mtask_map.get(jobid).get(trackername).add(t.getTaskID().getTaskID().getId());
*/
            }else if(jointype.equals("one2one")){
            	//remember the map task assignment, and assign reduce task based on it in future
            	scheduled_tracker_rtask_map.get(jobid).get(trackername).add(t.getTaskID().getTaskID().getId());

            }
            LOG.info("assigning task " + t.getTaskID() + " on " + trackername);
            // Don't assign map tasks to the hilt!
            // Leave some free slots in the cluster for future task-failures,
            // speculative tasks etc. beyond the highest priority job
            if (exceededMapPadding) {
              return 1;
            }
           
            // Try all jobs again for the next Map task 
            return -1;
          }
          
          LOG.error("New Node Or Rack Local Map Task failed!");
          
          if(jointype.equals("one2mul")){
        	// Try to schedule a node-local or rack-local Map task
        	  Integer target = scheduled_tracker_mtask_map.get(jobid).get(trackername).peekFirst();
        	  
        	  if(target == null){
        		  //all have been assigned, no more work, maybe it should help others to process
        		  LOG.info("all map tasks on tasktracker " + trackername + " have been processed");
        		  return -1;
        	  }else{
		          t = job.obtainNewNonLocalMapTask(taskTrackerStatus, numTaskTrackers, 
		        		  taskTrackerManager.getNumberOfUniqueHosts(), target);
        	  }
          }else{
	          t = job.obtainNewNonLocalMapTask(taskTrackerStatus, numTaskTrackers,
                        taskTrackerManager.getNumberOfUniqueHosts());
          }

          
          if (t != null) {
            assignedTasks.add(t);
            
            //new iteration job and the first task for a tasktracker
            //for one2mul case, we don't need to record the assignment, since we already made the assignment list beforehand
            if(jointype.equals("one2mul")){
            	//poll, remove
            	scheduled_tracker_mtask_map.get(jobid).get(trackername).pollFirst();
/*            	
            	//record the assignment list for map tasks
	            if(!tracker_mtask_map.get(jobid).containsKey(trackername)){
	            	LinkedList<Integer> tasklist = new LinkedList<Integer>();
	            	tracker_mtask_map.get(jobid).put(trackername, tasklist);
	            }
	            
	            tracker_mtask_map.get(jobid).get(trackername).add(t.getTaskID().getTaskID().getId());
	            */
            }else if(jointype.equals("one2one")){
            	scheduled_tracker_mtask_map.get(jobid).get(trackername).add(t.getTaskID().getTaskID().getId());
            }
            LOG.info("assigning task " + t.getTaskID() + " on " + trackername);

            // Don't assign map tasks to the hilt!
            // Leave some free slots in the cluster for future task-failures,
            // speculative tasks etc. beyond the highest priority job
            if (exceededMapPadding) {
              return 1;
            }
           
            // Try all jobs again for the next Map task 
            return -1;
          }
          
          return 0;
	  }
	  
	  private int assignReduceForIterative(TaskTracker taskTracker, JobInProgress job,
			  int numTaskTrackers, TaskTrackerStatus taskTrackerStatus, boolean exceededMapPadding, 
			  List<Task> assignedTasks) throws IOException{
		  Task t = null;
		  
    	  String iterativeAppID = job.getJobConf().getIterativeAlgorithmID();
    	  if(iterativeAppID.equals("none")){
    		  throw new IOException("please specify the iteration ID!");
    	  }
    	  
    	  String jointype = job.getJobConf().get("mapred.iterative.jointype");
    	  String trackername = taskTracker.getTrackerName();
    	  JobID jobid = job.getJobID();
    	  
    	  if(jointype.equals("one2mul")){
        	  Integer target = scheduled_tracker_rtask_map.get(jobid).get(trackername).peekFirst();
        	  
        	  if(target == null){
        		  //all have been assigned, no more work, maybe it should help others to process
        		  LOG.info("all reduce tasks on tasktracker " + trackername + " have been processed");
        		  return -1;
        	  }else{
		          t = job.obtainNewReduceTask(taskTrackerStatus, 
			                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts(), target);
        	  }
         }else if(jointype.equals("one2one")){
        	 
	       	  if(fail_tracker_rtask_map.containsKey(jobid) &&
	       			fail_tracker_rtask_map.get(jobid).containsKey(trackername) &&
	       			  !fail_tracker_rtask_map.get(jobid).get(trackername).isEmpty()){
	       		  
	       		  int target = fail_tracker_rtask_map.get(jobid).get(trackername).poll();
	       		  //this is a failed task, let's restart it from the jobinprogress
	       		  t = job.obtainFailedReduceTask(taskTrackerStatus, numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts());
	       	  }else{
		       	  //assign reduce task based on map task assignment
		       	  Integer target = scheduled_tracker_rtask_map.get(jobid).get(trackername).peekFirst();
	       		
		       	  if(target == null){
		       		  //all have been assigned, no more work, maybe it should help others to process
		       		  LOG.info("all reduce tasks on tasktracker " + trackername + " have been processed");
		       		  return -1;
		       	  }else{
				      t = job.obtainNewReduceTask(taskTrackerStatus, 
					                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts(), target);
		       	  }
	       	  }

         }else{
        	 t = job.obtainNewReduceTask(taskTrackerStatus, 
		                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts());
    	 }
    	  
    	  if(t != null){
    		  assignedTasks.add(t);
    		  
              if(jointype.equals("one2mul")){
              	//poll, remove
              	scheduled_tracker_rtask_map.get(jobid).get(trackername).pollFirst();
/*              	
              	//record the assignment list for map tasks
  	            if(!tracker_rtask_map.get(jobid).containsKey(trackername)){
  	            	LinkedList<Integer> tasklist = new LinkedList<Integer>();
  	            	tracker_rtask_map.get(jobid).put(trackername, tasklist);
  	            }
  	            
  	            tracker_mtask_map.get(jobid).get(trackername).add(t.getTaskID().getTaskID().getId());
*/
              }else if(jointype.equals("one2one")){
                	scheduled_tracker_rtask_map.get(jobid).get(trackername).pollFirst();
              }
              LOG.info("assigning task " + t.getTaskID() + " on " + trackername);
              
    		  return -1;
    	  }
    	  
    	  return 0;
	  }

	  private boolean exceededPadding(boolean isMapTask, 
	                                  ClusterStatus clusterStatus, 
	                                  int maxTaskTrackerSlots) { 
	    int numTaskTrackers = clusterStatus.getTaskTrackers();
	    int totalTasks = 
	      (isMapTask) ? clusterStatus.getMapTasks() : 
	        clusterStatus.getReduceTasks();
	    int totalTaskCapacity = 
	      isMapTask ? clusterStatus.getMaxMapTasks() : 
	                  clusterStatus.getMaxReduceTasks();

	    Collection<JobInProgress> jobQueue =
	      jobQueueJobInProgressListener.getJobQueue();

	    boolean exceededPadding = false;
	    synchronized (jobQueue) {
	      int totalNeededTasks = 0;
	      for (JobInProgress job : jobQueue) {
	        if (job.getStatus().getRunState() != JobStatus.RUNNING ||
	            job.numReduceTasks == 0) {
	          continue;
	        }

	        //
	        // Beyond the highest-priority task, reserve a little 
	        // room for failures and speculative executions; don't 
	        // schedule tasks to the hilt.
	        //
	        totalNeededTasks += 
	          isMapTask ? job.desiredMaps() : job.desiredReduces();
	        int padding = 0;
	        if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
	          padding = 
	            Math.min(maxTaskTrackerSlots,
	                     (int) (totalNeededTasks * padFraction));
	        }
	        if (totalTasks + padding >= totalTaskCapacity) {
	          exceededPadding = true;
	          break;
	        }
	      }
	    }

	    return exceededPadding;
	  }

	  @Override
	  public synchronized Collection<JobInProgress> getJobs(String queueName) {
	    return jobQueueJobInProgressListener.getJobQueue();
	  }  
	  
	  public void updateTaskStatus(JobID jobid, String trackername, Integer taskid){
		  LOG.info("some reduce task failed !! " + jobid + "\t" + trackername + "\t" + taskid);
		  
		  //scheduled_tracker_rtask_map.get(jobid).get(trackername).add(taskid);
		  
		  if(!fail_tracker_rtask_map.containsKey(jobid)){
			  fail_tracker_rtask_map.put(jobid, new HashMap<String, LinkedList<Integer>>());
		  }
		  
		  if(!fail_tracker_rtask_map.get(jobid).containsKey(trackername)){
			  fail_tracker_rtask_map.get(jobid).put(trackername, new LinkedList<Integer>());
		  }
		  
		  fail_tracker_rtask_map.get(jobid).get(trackername).add(taskid);
	  }
}
