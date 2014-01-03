/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.search;

import java.util.Map.Entry;
import java.util.WeakHashMap;

/**
 * Utility class which efficiently monitors several threads performing
 * time-limited activities Client threads call start(maxTimeMilliseconds) and
 * stop() to denote start and end of time-limited activity and calls to
 * checkForTimeout() will throw a {@link ActivityTimedOutException} if that
 * thread has exceeded the maxTimeMilliseconds defined in the start call.
 * 
 * Any class can call the static checkForTimeout method making it easy for
 * objects shared by threads to simply check if the current thread is
 * over-running in its activity.
 * 
 * The checkForTimeout call is intended to be very lightweight (no
 * synchronisation) so can be called in tight loops.
 * 
 */
public class ActivityTimeMonitor {
    
    /**
     * An interface for listening to significant state changes in the timeout monitor.
     * All calls to this interface are single-threaded and relate to only badly-behaved threads
     */
    public interface TimeoutListener{
        

        /**
         * Called as soon as it is detected that an activity has overrun its allotted time. Only called 
         * once per over-running activity.
         * @param badThread The thread previously registered as performing the time-limited activity
         * @param scheduledTimeout The system time (in milliseconds) by which the activity should have been completed
         * @param currentOverrun The overrun in milliseconds at this point of detection
         */
        void overrunningActivityDetected(Thread badThread, long scheduledTimeout, long currentOverrun);

        /**
         * Called when an over-running thread has its scheduled time limit revised 
         * @param thread the thread with the over-running activity
         * @param newScheduledTimeout The new timeout expressed as System time in milliseconds
         */
        void overRunningActivityExtended(Thread thread, long newScheduledTimeout);
        /**
         * Called when an over-running thread has invoked checkForTimeout and is about to have an 
         * {@link ActivityTimedOutException} thrown in response to try shortcut its processing. 
         * @param badThread The thread that has allowed an activity to overrun.
         * @param scheduledTimeout the system time in milliseconds by which badThread should have completed an activity
         * @param overrun the current overrun in milliseconds
         */
        void preActivityAbort(Thread badThread, long scheduledTimeout, long overrun);
        /**
         * Called if an over-running thread calls stop() to end the timed activity before any timeout exception was
         * triggered to abort it.
         * @param badThread The thread with the over-running activity
         */
        void overRunningActivityStopped(Thread badThread);        
        /**
         * Called when all over-running activities have either a) been stopped naturally b) triggered to abort with an exception or
         * c) had their activity timeout extended.
         * While the {@link ActivityTimeMonitor} has any timeout condition in play all calls to checkForTimeout will run slower
         * than usual as they will involve synchronization to check if the current thread is part of the timed-out set of threads.
         */
        void allTimeoutsCleared();
    }
    

    // List of active threads that have called start(maxTimeMilliseconds)
    static private WeakHashMap<Thread, Long> timeLimitedThreads = new WeakHashMap<Thread, Long>();
    // The earliest timeout we can anticipate
    static private volatile long firstAnticipatedTimeout = Long.MAX_VALUE;
    // The volatile variable that provides a very fast means of determining if anything has out-stayed its welcome
    static public volatile boolean anActivityHasTimedOut;
    // Set of threads that are known to have overrun
    static private WeakHashMap<Thread, Long> timedOutThreads = new WeakHashMap<Thread, Long>();

    // an internal thread that monitors timeout activity
    private static TimeoutMonitorThread timeoutMonitorThread;
    private static TimeoutListener timeoutListener;


    

    /**
     * Called by client thread that is starting some time-limited activity. The
     * stop method MUST ALWAYS be called after calling this method - use of
     * try-finally is highly recommended.
     * 
     * 
     * @param maxTimeMilliseconds
     *            dictates the maximum length of time that this thread is
     *            permitted to execute a task.
     */
    public static final void start(long maxTimeMilliseconds) {
        long scheduledTimeout = System.currentTimeMillis() + maxTimeMilliseconds;
        setThreadTimeout(scheduledTimeout, Thread.currentThread());
    }
  
    /**
     * Called to set a time limit for a thread. This can be used to revise the scheduled timeout
     * for a thread that has already been assigned a timeout setting. 
     * 
     * @param scheduledTimeout
     *            The time at which "thread" will be deeemd to have timed out
     * 
     * @param thread
     *            The thread which is the subject of the timeout
     */
    public static final void setThreadTimeout(long scheduledTimeout, Thread thread) {
        synchronized (timeLimitedThreads) {
            // store the scheduled point in time when the thread should fail
            timeLimitedThreads.put(thread, new Long(scheduledTimeout));
            
            //It is possible that a sysadmin uses an admin console to ask for an active thread to end immediately 
            // but before the console has a chance to invoke this setThreadTimeout method the thread times out naturally
            // due to the existing timeout setting. In this case we would need to reset the error condition
            if (timedOutThreads.remove(thread) != null) {
                if(timeoutListener!=null){
                    timeoutListener.overRunningActivityExtended(thread,scheduledTimeout);
                }                                                        
                checkOverrunsStillActive();
            }

            // if we are not in the middle of handling a timeout
            if (!anActivityHasTimedOut) {
                // check to see if this is now the first thread expected to timeout...
                if (scheduledTimeout < firstAnticipatedTimeout) {
                    firstAnticipatedTimeout = scheduledTimeout;

                    // Reset TimeOutMonitorThread with new earlier, more
                    // aggressive deadline on which to wait
                    timeLimitedThreads.notify();
                }
            }
        }
    }
    

    
    /**
     * MUST ALWAYS be called by clients after calling start.
     */
    public static final void stop() {
        Thread currentThread = Thread.currentThread();
        
        synchronized (timeLimitedThreads) {
            Long thisTimeOut = timeLimitedThreads.remove(currentThread);
            // Choosing not to throw an exception if thread has timed out - we
            // don't punish overruns at last stage
            // but I guess that could be an option if you were feeling malicious
            if (timedOutThreads.remove(currentThread) != null) {
                if(timeoutListener!=null){
                    timeoutListener.overRunningActivityStopped(currentThread);
                }
                checkOverrunsStillActive();
            }

            if (thisTimeOut != null) // This thread may have timed out and been
                                     // removed from timeLimitedThreads
            {
                if (thisTimeOut.longValue() <= firstAnticipatedTimeout) {
                    // this was the first thread expected to timeout -
                    // resetFirstAnticipatedFailure
                    resetFirstAnticipatedFailure();
                }
            }
        }
    }

    private static void resetFirstAnticipatedFailure() {
        synchronized (timeLimitedThreads) {
            // find out which is the next candidate for failure

            // TODO Not the fastest conceivable data structure to achieve this.
            // TreeMap was suggested as alternative for timeLimitedThreads (
            // http://tinyurl.com/rd8xro ) because
            // it is sorted but TreeMaps sorted by key not value. Life gets
            // awkward in trying sort by value - see ....
            // http://tinyurl.com/pjb6oe .. which makes the point that
            // key.equals must be consistent with
            // compareTo logic so not possible to use a single structure for
            // fast hashcode lookup on a Thread key
            // AND maintain a sorted list based on timeout value.
            // However not likely to be thousands of concurrent threads at any
            // one time so maybe this is
            // not a problem - we only iterate over all of timeLimitedThreads in
            // 2 scenarios
            // 1) Stop of a thread activity that is the current earliest
            // expected timeout (relatively common)
            // 2) In the event of a timeout (hopefully very rare).
            // Although 1) is a relatively common event it is certainly not in
            // the VERY frequently called checkForTimeout() 
            long newFirstNextTimeout = Long.MAX_VALUE;
            for (Entry<Thread, Long> timeout : timeLimitedThreads.entrySet()) {
                long timeoutTime = timeout.getValue().longValue();
                if (timeoutTime < newFirstNextTimeout) {
                    newFirstNextTimeout = timeoutTime;
                }
            }
            firstAnticipatedTimeout = newFirstNextTimeout;

            // Reset TimeoutThread with lowest timeout from remaining active threads
            timeLimitedThreads.notify();
        }
    }
    
    // Ensures all threads on the naughty list are still active. If not, clears the error status 
    private static boolean checkOverrunsStillActive(){
        for (Thread thread : timedOutThreads.keySet()) {
            if(thread.isAlive()){
                return true;
            }
        }
        timedOutThreads.clear();
        anActivityHasTimedOut = false;
        if(timeoutListener!=null){
            timeoutListener.allTimeoutsCleared();
        }        
        return false;
    }

    /**
     * Checks to see if this thread has exceeded its pre-determined timeout.
     * This is intended to be a light-weight call and can be called at high volume.
     * Throws {@link ActivityTimedOutException} RuntimeException in the event of
     * any timeout. 
     * A timeout exception will only be thrown once for a given thread/activity so
     * it is the caller's responsibility to ensure that all calls to this method
     * take the appropriate tidy-up actions to abort the current activity.
     */
    public static final void checkForTimeout() {

        if (ActivityTimeMonitor.anActivityHasTimedOut) {
            checkTimeoutIsThisThread();
        }
    }

    private static final void checkTimeoutIsThisThread() {
        Thread currentThread = Thread.currentThread();
        synchronized (timeLimitedThreads) {

            Long thisTimeOut = timedOutThreads.remove(currentThread);
            if (thisTimeOut != null) {
                checkOverrunsStillActive();
                long now = System.currentTimeMillis();
                long overrun = now - thisTimeOut.longValue();
                if(timeoutListener!=null){
                    timeoutListener.preActivityAbort(currentThread, thisTimeOut.longValue(), overrun);
                }
                throw new ActivityTimedOutException("Thread " + currentThread + " has timed out, overrun =" + overrun + " ms", overrun);

            }
        }
    }
    
    /**
     *  Utility method for testing if the current thread has a timelimit set.
     *  This is a relatively expensive call so should not be called frequently  
     *  - use checkForTimeout() for high-frequency calls.
     * @return The system time in milliseconds when this thread is expected to timeout or
     *         zero if no timeout has been set.
     */
    public static long getCurrentThreadTimeout(){
        Long timeout=null;
        synchronized (timeLimitedThreads) {
            timeout=timeLimitedThreads.get(Thread.currentThread());
            if(timeout==null){
                timeout=timedOutThreads.get(Thread.currentThread());
            }
        }
        return timeout==null?0:timeout.longValue();
    }
    
    
    
    //TODO should allow for multiple listeners?
    public static void setTimeoutListener(TimeoutListener newListener){
        timeoutListener=newListener;
    }
    
    public static int getCurrentNumActivities() {
        synchronized (timeLimitedThreads) {
            return timeLimitedThreads.size()+timedOutThreads.size();
        }
    }    

    static {
        timeoutMonitorThread = new TimeoutMonitorThread();
        timeoutMonitorThread.setDaemon(true);
        timeoutMonitorThread.start();
    }

    //A background thread that only wakes when
    private static final class TimeoutMonitorThread extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    synchronized (timeLimitedThreads) {
                        long now = System.currentTimeMillis();
                        long waitTime = firstAnticipatedTimeout - now;
//                        System.out.println("Monitor checking for timeout <" +firstAnticipatedTimeout);
                        if (waitTime <= 0) {
                            // Something may have timed out - check all threads,
                            // adding all currently timed out threads to
                            // timedOutThreads
                            long newFirstAnticipatedTimeout = Long.MAX_VALUE;
                            for (Entry<Thread, Long> threadAndTime : timeLimitedThreads.entrySet()) {
                                long timeoutTime = threadAndTime.getValue().longValue();
                                if (timeoutTime <= now) {
                                    // thread has timed out
                                    if(timeoutListener!=null){
                                        timeoutListener.overrunningActivityDetected(threadAndTime.getKey(), timeoutTime, now-timeoutTime);
                                    }                                        
                                    timedOutThreads.put(threadAndTime.getKey(), timeoutTime);
                                    anActivityHasTimedOut = true;
                                } else {
                                    // re-assess time of next potential failure
                                    newFirstAnticipatedTimeout = Math.min(newFirstAnticipatedTimeout, timeoutTime);
                                }
                            }
                            if (anActivityHasTimedOut) {
                                if(checkOverrunsStillActive()){
                                    //ensure all timed-out threads are removed from active list
                                    //(we could not remove on-discovery in above loop due to concurrent
                                    //modification issues with the collection we were looping around).
                                    for (Thread timedoutThread : timedOutThreads.keySet()) {
                                        timeLimitedThreads.remove(timedoutThread);
                                    }
                                }
                            }
                            firstAnticipatedTimeout = newFirstAnticipatedTimeout;
                            waitTime = firstAnticipatedTimeout - now;
                        }
                        // Sleep until the next anticipated time of failure
                        // (or, hopefully more likely) until
                        // notify-ed of a new next anticipated time of failure
                        
                        timeLimitedThreads.wait(waitTime);
                    }

                } catch (InterruptedException e) {
                  
                }
            }
        }


    }

 }