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

    private final Queue<PubSubMessage> queue;

    private final List<Timestamp> history;

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
        synchronized (TimeDelayQueue.class){
            removeExpiredTransientMsg();
            Timestamp now = new Timestamp(System.currentTimeMillis());
            if (queue.contains(msg)) {
                return false;
            } else {
                queue.add(msg);
                history.add(now); // record the action
                totalMsgCount ++; // update message count
            }
            return true;
        }
    }

    /**
     * Get the count of the total number of messages processed
     * by this TimeDelayQueue
     * @return totalMsgCount
     */
    public long getTotalMsgCount() {
        return totalMsgCount;
    }

    // return the next message and PubSubMessage.NO_MSG
    // if there is no suitable message
    public PubSubMessage getNext() {
        synchronized (TimeDelayQueue.class){
            Timestamp now = new Timestamp(System.currentTimeMillis());
            removeExpiredTransientMsg();
            PubSubMessage next;
            try {
                next = queue.element();
            } catch (NoSuchElementException e){
                return PubSubMessage.NO_MSG;
            }

            if ((next.getTimestamp().getTime() + delay) >= now.getTime()){
                queue.remove(next);
                history.add(now);
                return next;
            } else {
                return PubSubMessage.NO_MSG;
            }
        }
    }

    // return the maximum number of operations
    // performed on this TimeDelayQueue over
    // any window of length timeWindow
    // the operations of interest are add and getNext
    public int getPeakLoad(int timeWindow) {
        int peakLoad = 0;
        history.sort(Timestamp::compareTo);
        for (int i = 0; i < history.size(); i++) {
            long initialTime = history.get(i).getTime();
            int currentPeakLoad = 0;
            for (int j = i; j < history.size(); j++) {
                if (history.get(j).getTime() - initialTime <= timeWindow){
                    currentPeakLoad ++;
                }
            }
            if (currentPeakLoad > peakLoad) {
                peakLoad = currentPeakLoad;
            }
        }
        return peakLoad;
    }

    /**
     * remove expired transient messages
     */
    private void removeExpiredTransientMsg() {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        List<PubSubMessage> toBeRemoved = new ArrayList<>();
        queue.forEach(msg -> {
            if (msg.isTransient()) {
                long expireTime = msg.getTimestamp().getTime() + ((TransientPubSubMessage) msg).getLifetime();
                if (expireTime > now.getTime()){
                    toBeRemoved.add(msg);
                }
            }
        });
        queue.removeAll(toBeRemoved);
    }

}
