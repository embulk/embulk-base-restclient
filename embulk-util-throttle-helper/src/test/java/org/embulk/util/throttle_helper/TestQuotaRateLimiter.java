package org.embulk.util.throttle_helper;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author instcode
 */
public class TestQuotaRateLimiter
{
    @Test
    public void testRateLimitEqualsToThreadCount() throws InterruptedException, ExecutionException
    {
        verifyWithMultipleThreads(10, 1, 5, 25);
    }

    @Test
    public void testRateLimitLowerThanThreadCount() throws InterruptedException, ExecutionException
    {
        verifyWithMultipleThreads(65, 2, 10, 80);
    }

    @Test
    public void testRateLimitGreaterThanThreadCount() throws InterruptedException, ExecutionException
    {
        verifyWithMultipleThreads(150, 1, 5, 500);
    }

    @Test
    public void testRateLimitWithNothingInMind() throws InterruptedException, ExecutionException
    {
        verifyWithMultipleThreads(230, 3, 8, 320);
    }

    private int verifyWithMultipleThreads(int rateLimit, int timeWindowInSeconds, int threadCount, final int taskCount) throws InterruptedException, ExecutionException
    {
        // Sometimes, the real completion time is very near to the estimation completion time, i.e. 4992 vs 5000,
        // due to the overhead of function calling doesn't take into account and it causes the assertion failed.
        // This error value (ms) is to make sure the error can be ignorable for such cases
        long errorInMs = 10;
        long startTime = System.nanoTime();
        final QuotaRateLimiter rateLimiter = new QuotaRateLimiter(rateLimit, timeWindowInSeconds);
        executeWithThreadPool(rateLimiter, threadCount, taskCount);
        int estimatedCompletionTimeInMs = (int) Math.floor((taskCount * 2 - rateLimit) * timeWindowInSeconds * 1000.0d / rateLimit);
        Assert.assertTrue("Should take at least " + estimatedCompletionTimeInMs + "ms to complete",
                System.nanoTime() - startTime >= (estimatedCompletionTimeInMs - errorInMs) * 1e6);
        // Sleep for another time window
        Thread.sleep(timeWindowInSeconds * 1000);
        rateLimiter.acquire(2);
        Assert.assertEquals("Remaining limit should be reset to the max minus 2",
                            rateLimit - 2, rateLimiter.getRemainingLimitForTesting());
        return estimatedCompletionTimeInMs / 1000;
    }

    private void executeWithThreadPool(final QuotaRateLimiter rateLimiter, int threadCount, int taskCount) throws InterruptedException, ExecutionException
    {
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        Future[] futures = new Future[taskCount];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = executorService.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    rateLimiter.acquire(2);
                }
            });
        }
        for (Future future : futures) {
            future.get();
        }
        executorService.shutdown();
    }
}
