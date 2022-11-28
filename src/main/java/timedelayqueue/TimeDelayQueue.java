package timedelayqueue;

import java.sql.Timestamp;
import java.util.*;

// TODO: write a description for this class
// TODO: complete all methods, irrespective of whether there is an explicit TODO or not
// TODO: write clear specs
// TODO: State the rep invariant and abstraction function
// TODO: what is the thread safety argument?
public class TimeDelayQueue {
    /* RI
       AF
     */

    private final int delay;

    private Queue<PubSubMessage> queue;

    private List<Timestamp> history;

    private long totalMsgCount;

    // a comparator to sort messages
    private class PubSubMessageComparator implements Comparator<PubSubMessage> {
        public int compare(PubSubMessage msg1, PubSubMessage msg2) {
            return msg1.getTimestamp().compareTo(msg2.getTimestamp());
        }
    }

    /**
     * Create a new TimeDelayQueue
     * @param delay the delay, in milliseconds, that the queue can tolerate, >= 0
     */
    public TimeDelayQueue(int delay) {
        this.delay = delay;
        queue = new PriorityQueue<>(new PubSubMessageComparator());
        history = new ArrayList<>();
        totalMsgCount = 0L;
    }

    // add a message to the TimeDelayQueue
    // if a message with the same id exists then
    // return false
    public boolean add(PubSubMessage msg) {
        //add operation to history
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        history.add(currentTime);

        if(queue.contains(msg)) {
            return false;
        }else{
            queue.offer(msg);
            totalMsgCount++;

            return true;
        }
    }

    /**
     * Get the count of the total number of messages processed
     * by this TimeDelayQueue
     * @return
     */
    public long getTotalMsgCount() {
        return totalMsgCount;
    }

    // return the next message and PubSubMessage.NO_MSG
    // if there is ni suitable message
    public PubSubMessage getNext() {
        //add operation to history
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        history.add(currentTime);

        //poll next message
        PubSubMessage next = queue.poll();

        while(next != null && isExpired(next)){//removes expired message and get the next one
            next = queue.poll();
        }

        //no message left in the queue
        if(next == null){
            return PubSubMessage.NO_MSG;
        }

        //check if the message has been delayed for long enough
        long timeStamp = next.getTimestamp().getTime();
        long now = currentTime.getTime();

        if(now - timeStamp > delay){
            return next;
        }else{
            return PubSubMessage.NO_MSG;
        }
    }

    // return the maximum number of operations
    // performed on this TimeDelayQueue over
    // any window of length timeWindow
    // the operations of interest are add and getNext
    public int getPeakLoad(int timeWindow) {

        return -1;
    }

    //helper//

    /**
     * check if a message is expired
     * @param m a PubSubMessage
     * @return true if m is a TransientPubSubMessage, false otherwise
     */
    private boolean isExpired(PubSubMessage m){
        if (!m.isTransient()){
            return false;
        }else {
            int lifeTime = ((TransientPubSubMessage) m).getLifetime();
            long timeStamp = m.getTimestamp().getTime();
            long now = new Timestamp(System.currentTimeMillis()).getTime();

            if(timeStamp + lifeTime < now){
                return true;
            }else{
                return false;
            }
        }
    }

}
