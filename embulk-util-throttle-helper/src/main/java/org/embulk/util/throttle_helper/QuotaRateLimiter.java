package org.embulk.util.throttle_helper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A rate limiter that allows a specific number of operations happen within
 * a time window and its rate limit is only reset at the beginning of the
 * next time window.
 * <p>
 * The time window is currently in second resolution to make sure unused permit
 * doesn't get lost in situation when a thread, that must sleep in more than 2
 * time windows, wakes up before all other threads, and reset the limits.
 *
 * @author instcode
 * @author thangnc
 */
@ThreadSafe
public class QuotaRateLimiter
{
    /**
     * This is to prevent multiple threads from updating {@link #remainingLimit} &
     * {@link #lastRefreshTime} wrongly
     */
    private final Lock lock = new ReentrantLock();

    /**
     * The number of permits per the given time window
     */
    private final int rateLimit;

    /**
     * The amount of time that affects the rate limit, i.e. the number of
     * operations should not exceed the rate limit within this time window.
     * This is also the amount of time that the rate limit gets refreshed.
     */
    private final int timeWindowInMillis;

    /**
     * The number of available permits to give. If it is a positive number, it's
     * the number of remaining permits for the current time window. Otherwise, it
     * indicates that there are pending reserved permits that may affect the time to
     * wait of incoming requests. For example, assume that the rate limit is 5 in
     * 10 seconds as starting point, then if there are 15 requests come at once,
     * there will be at least 5 requesters should have to wait for 20 seconds and
     * the remaining limit is -5
     */
    private volatile int remainingLimit;

    /**
     * The last rate limit refresh time (ms since epoch)
     */
    private volatile long lastRefreshTime;

    /**
     * Instantiates a new Time window rate limiter.
     *
     * @param rateLimit           the rate limit
     * @param timeWindowInSeconds the time window in seconds
     */
    public QuotaRateLimiter(int rateLimit, int timeWindowInSeconds)
    {
        this(rateLimit, rateLimit, timeWindowInSeconds, System.currentTimeMillis());
    }

    /**
     * Instantiates a new Time window rate limiter.
     *
     * @param rateLimit           the rate limit
     * @param remainingLimit      the remaining limit
     * @param timeWindowInSeconds the time window in seconds
     * @param lastRefreshTime     the last refresh time
     */
    public QuotaRateLimiter(int rateLimit, int remainingLimit, int timeWindowInSeconds, long lastRefreshTime)
    {
        this.rateLimit = rateLimit;
        this.remainingLimit = remainingLimit;
        this.timeWindowInMillis = timeWindowInSeconds * 1000;
        this.lastRefreshTime = lastRefreshTime;
    }

    /**
     * Request a number of permits and may be blocking until the request is granted.
     *
     * @param permits The permits
     * @return The time spent sleeping in milliseconds
     */
    public long acquire(int permits)
    {
        long millisToSleepTime = reserveAndGetWaitTime(permits);
        Uninterruptibles.sleepUninterruptibly(millisToSleepTime, TimeUnit.MILLISECONDS);
        return millisToSleepTime;
    }

    /**
     * Gets remaining limit
     *
     * @return The remaining limit
     */
    @VisibleForTesting
    public int getRemainingLimitForTesting()
    {
        return remainingLimit;
    }

    /**
     * Reserve a number of permits for the current requester
     *
     * @param permits Number of permits is requested
     * @return The amount of time to wait for the available permits in milliseconds
     */
    private long reserveAndGetWaitTime(int permits)
    {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            if (now - lastRefreshTime >= timeWindowInMillis) {
                int numberOfTimeWindows = (int) ((now - lastRefreshTime) / timeWindowInMillis);
                // Refill rate limit amount for every time window is passed
                // but it should not surpass max rate limit
                remainingLimit = Math.min(rateLimit, remainingLimit + rateLimit * numberOfTimeWindows);
                lastRefreshTime += timeWindowInMillis * numberOfTimeWindows;
            }
            remainingLimit = remainingLimit - permits;
            if (remainingLimit >= 0) {
                // No need to wait
                return 0;
            }
            return millisToTheNextAvailablePermits(permits);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Calculate the amount of time to wait until the number of permits requested is
     * available to acquire by the current requester
     */
    private long millisToTheNextAvailablePermits(int permits)
    {
        int numberOfTimeWindows = 1;
        if (remainingLimit < 0) {
            // Add an extra time wait basing on how fast the rate limit
            // is refreshed. Note that remainingLimit <= -1 here
            numberOfTimeWindows -= (remainingLimit + permits) / rateLimit;
        }
        long amountOfReservedTime = timeWindowInMillis * numberOfTimeWindows;
        long elapsedTimeSinceTheLastRefresh = System.currentTimeMillis() - lastRefreshTime;
        return amountOfReservedTime - elapsedTimeSinceTheLastRefresh;
    }
}
