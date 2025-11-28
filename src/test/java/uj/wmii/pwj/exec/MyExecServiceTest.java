package uj.wmii.pwj.exec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class MyExecServiceTest {

    class MyRunnable implements Runnable {
        private long computeTime;
        public int result;
        public boolean finished;

        MyRunnable(int setResult, long setComputeTime) {
            computeTime = setComputeTime;
            result = setResult;
            finished = false;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(computeTime);
                finished = true;
                addResult(result);
            }
            catch ( Exception e ) {

            }
        }
    }

    class MyCallable implements Callable<Integer> {
        private long computeTime;
        public int result;
        public boolean finished;

        MyCallable(int setResult, long setComputeTime) {
            computeTime = setComputeTime;
            result = setResult;
            finished = false;
        }

        @Override
        public Integer call() throws Exception {
            Thread.sleep(computeTime);
            finished = true;
            addResult(result);
            return result;
        }
    }

    class InterruptingCallable implements Callable<Integer> {
        private final long delayTime;
        InterruptingCallable(long setDelayTime) {
            this.delayTime = setDelayTime;
        }

        @Override
        public Integer call() throws Exception {
            Thread.sleep(delayTime);
            throw new InterruptedException();
        }
    }

    class ErrorCallable implements Callable<Integer> {
        private final long delayTime;
        ErrorCallable(long setDelayTime) {
            this.delayTime = setDelayTime;
        }

        @Override
        public Integer call() throws Exception {
            Thread.sleep(delayTime);
            throw new Exception();
        }
    }

    private MyExecService service;
    List<Integer> resultOrder;

    synchronized void addResult(int a) {
        resultOrder.add(a);
    }

    boolean evaluateOrder(int[] tab) {
        for( int i : resultOrder ) {
            if ( i == tab.length ) return true;
            if ( tab[i] != resultOrder.get(i) ) return false;
        }
        return true;
    }

    @BeforeEach
    void Prepare() {
        service = MyExecService.newInstance(4);
        resultOrder = new LinkedList<>();
    }

    //region Submit Tests
    @Test
    void assertSubmitCompletionOrder() {
        MyRunnable task1 = new MyRunnable(1, 10);
        MyRunnable task2 = new MyRunnable(2, 50);
        MyRunnable task3 = new MyRunnable(3, 30);

        service.submit(task1);
        service.submit(task2);
        service.submit(task3);

        try {
            Thread.sleep(60);
        }
        catch (Exception e) {
            fail("Unexpected error on Thread.sleep");
        }
        assertTrue(evaluateOrder(new int[]{1, 3, 2}), "Failed on multitasking processing order");
    }

    @Test
    void assertRejectTaskAfterShutdown() {
        MyRunnable task = new MyRunnable(1, 2);
        service.shutdown();
        assertThrows(RejectedExecutionException.class, () -> service.submit(task));
    }

    @Test
    void assertProperSubmitReturnValue() throws ExecutionException, InterruptedException {
        MyRunnable task = new MyRunnable(1, 1);
        Integer resultValue = 67;
        Future<Integer> resultSource = service.submit(task, resultValue);

        assertEquals(resultValue, resultSource.get());
    }

    @Test
    void assertNullSubmitReturnValue() throws ExecutionException, InterruptedException {
        MyRunnable task = new MyRunnable(123, 1);
        Future<?> resultSource = service.submit(task);
        assertNull(resultSource.get());
    }

    @Test
    void assertTaskWaiting() {
        MyRunnable longTask1 = new MyRunnable(1, 1000);
        MyRunnable longTask2 = new MyRunnable(2, 1000);
        MyRunnable longTask3 = new MyRunnable(3, 1000);
        MyRunnable longTask4 = new MyRunnable(4, 1000);
        MyRunnable quickTask = new MyRunnable(5, 1);

        service.submit(longTask1);
        service.submit(longTask2);
        service.submit(longTask3);
        service.submit(longTask4);
        service.submit(quickTask);

        assertFalse(quickTask.finished, "Task finished while all threads should be occupied");
    }
    //endregion

    //region InvokeAll Tests
    @Test
    void tryInvokeNull() throws InterruptedException {
        assertThrows(NullPointerException.class, () -> service.invokeAll(null));
    }

    @Test
    void tryInvokeMany() throws InterruptedException, ExecutionException {
        Collection<Callable<Integer>> tasks = new ArrayList<>();
        tasks.add( new MyCallable(1, 10) );
        tasks.add( new MyCallable(2, 114) );
        tasks.add( new MyCallable(3, 59) );
        tasks.add( new MyCallable(4, 2) );
        tasks.add( new MyCallable(5, 82) );
        tasks.add( new MyCallable(6, 6) );
        tasks.add( new MyCallable(7, 23) );
        tasks.add( new MyCallable(8, 25) );
        tasks.add( new MyCallable(9, 19) );

        List<Future<Integer>> results = service.invokeAll(tasks);

        for ( Future<Integer> check : results ) {
            assertTrue(check.isDone(), "Task failed to compute: number " + check.get()
            + "\nWhere order for completed tasks is: " + resultOrder.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(", "))
            );
        }
    }

    @Test
    void handleTaskThrowingException() throws InterruptedException {
        MyCallable completingCallable = new MyCallable(1, 1);
        InterruptingCallable errorCallable = new InterruptingCallable(20);
        MyCallable incompleteCallable = new MyCallable(1, 100);

        Collection<Callable<Integer>> tasks = new ArrayList<>();
        tasks.add(completingCallable);
        tasks.add(errorCallable);
        tasks.add(incompleteCallable);

        try {
            List<Future<Integer>> results = service.invokeAll(tasks);
        }
        catch (InterruptedException e) {
            assertTrue(completingCallable.finished, "Task failed to finish despite finishing in time before cancel");
            assertFalse(incompleteCallable.finished, "Task managed to finish despite throwing ");
            return;
        }
        fail("invokeAll should have thrown an exception");
    }

    @Test
    void testTimeout() throws InterruptedException {
        MyCallable completedTask = new MyCallable(1, 1);
        MyCallable tooLongTask = new MyCallable(1, 100);

        Collection<MyCallable> tasks = new ArrayList<>();
        tasks.add(completedTask);
        tasks.add(tooLongTask);

        List<Future<Integer>> results = service.invokeAll(tasks, 25, TimeUnit.MILLISECONDS);

        assertTrue(results.get(0).isDone(), "Task failed to finish despite fitting within the timeout limit");
        assertTrue(results.get(1).isDone(), "Cancelled task should still give true on .isDone()");
        assertTrue(completedTask.finished);
        assertFalse(tooLongTask.finished);
    }

    @Test
    void testAllTimeout() throws InterruptedException {
        MyCallable task1 = new MyCallable(1, 100);
        MyCallable task2 = new MyCallable(2, 100);
        MyCallable task3 = new MyCallable(3, 100);
        MyCallable task4 = new MyCallable(4, 100);
        MyCallable task5 = new MyCallable(5, 100);

        Collection<MyCallable> tasks = new ArrayList<>();
        tasks.add(task1);
        tasks.add(task2);
        tasks.add(task3);
        tasks.add(task4);
        tasks.add(task5);

        List<Future<Integer>> results = service.invokeAll(tasks, 25, TimeUnit.MILLISECONDS);

        assertTrue(resultOrder.isEmpty(), "Returning list is not empty despite all tasks taking too long");
        assertFalse(task1.finished, "Task managed to finish despite taking longer than timeout limit");
        assertFalse(task2.finished, "Task managed to finish despite taking longer than timeout limit");
        assertFalse(task3.finished, "Task managed to finish despite taking longer than timeout limit");
        assertFalse(task4.finished, "Task managed to finish despite taking longer than timeout limit");
        assertFalse(task5.finished, "Task managed to finish despite taking longer than timeout limit");
    }
    //endregion

    //region InvokeAny Tests
    @Test
    void assertTaskCompletion() throws ExecutionException, InterruptedException {
        ArrayList<Callable<Integer>> tasks = new ArrayList<>();

        MyCallable task1 = new MyCallable(1, 10);
        MyCallable task2 = new MyCallable(2, 10);
        MyCallable task3 = new MyCallable(3, 1);
        MyCallable task4 = new MyCallable(4, 10);

        tasks.add(task1);
        tasks.add(task2);
        tasks.add(task3);
        tasks.add(task4);

        Integer result = service.invokeAny(tasks);
        assertNotNull(result);
        assertEquals(3, result);
        assertFalse(task1.finished);
        assertFalse(task2.finished);
        assertTrue(task3.finished);
        assertFalse(task4.finished);
    }

    @Test
    void testBlockedShortTask() throws ExecutionException, InterruptedException {
        ArrayList<Callable<Integer>> tasks = new ArrayList<>();

        MyCallable task1 = new MyCallable(1, 50);
        MyCallable task2 = new MyCallable(2, 50);
        MyCallable task3 = new MyCallable(3, 50);
        MyCallable task4 = new MyCallable(4, 50);
        MyCallable task5 = new MyCallable(0, 10);

        tasks.add( task1 );
        tasks.add( task2 );
        tasks.add( task3 );
        tasks.add( task4 );
        tasks.add( task5 );

        assertNotEquals(0, service.invokeAny(tasks), "Task that was supposed to be blocked finished before all others");
        assertFalse(task5.finished, "Task finished after invokeAny got its result");
    }

    @Test
    void testTaskThrowingException() {
        ArrayList<Callable<Integer>> tasks = new ArrayList<>();

        MyCallable taskCompleted = new MyCallable(1, 10);
        InterruptingCallable throwingTask = new InterruptingCallable(1);
        MyCallable taskIncompleted = new MyCallable(2, 50);

        tasks.add(taskCompleted);
        tasks.add(throwingTask);
        tasks.add(taskIncompleted);

        Integer result = null;
        try {
            result = service.invokeAny(tasks);
        }
        catch (ExecutionException e) {
            fail("Failed to return any of the successful tasks");
        }
        catch (Exception e) {
            fail("Unknown exception source thrown");
        }
        assertNotNull(result, "Failed to return value");
        assertEquals(1, result);
        assertTrue(taskCompleted.finished);
        assertFalse(taskIncompleted.finished);
    }

    @Test
    void testAllTasksThrowingException() {
        ArrayList<Callable<Integer>> tasks = new ArrayList<>();

        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );

        try {
            service.invokeAny(tasks);
        }
        catch ( ExecutionException e ) {
            return;
        }
        catch (Exception e) {
            fail("Unexpected exception");
        }

        fail("No exception despite no successful tasks");
    }

    @Test
    void testTaskTimeout() {
        ArrayList<Callable<Integer>> tasks = new ArrayList<>();

        MyCallable task1 = new MyCallable(1, 100);
        MyCallable task2 = new MyCallable(2, 150);

        tasks.add( task1 );
        tasks.add( task2 );

        try {
            service.invokeAny(tasks, 25, TimeUnit.MILLISECONDS);
        }
        catch ( TimeoutException e ) {
            return;
        }
        catch (Exception e) {
            fail("Unexpected exception");
        }

        fail("No exception was thrown");
    }

    @Test
    void testTaskCompletionBeforeTimeout() {
        ArrayList<Callable<Integer>> tasks = new ArrayList<>();

        MyCallable task1 = new MyCallable(1, 30);
        MyCallable task2 = new MyCallable(2, 15);

        tasks.add( task1 );
        tasks.add( task2 );

        Integer result = null;
        try {
            result = service.invokeAny(tasks, 500, TimeUnit.MILLISECONDS);
        }
        catch ( TimeoutException e ) {
            fail("Invocation timed out");
        }
        catch (Exception e) {
            fail("Unexpected exception");
        }

        assertEquals(2, result, "Shorter task did not finish first");
    }

    @Test
    void testAllTasksThrowingExceptionBeforeTimeout() {
        ArrayList<Callable<Integer>> tasks = new ArrayList<>();

        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        tasks.add( new InterruptingCallable(5) );
        try {
            service.invokeAny(tasks, 25, TimeUnit.MILLISECONDS);
        }
        catch ( TimeoutException e ) {
            fail("There should not be a timeout; all tasks within timeout limit");
        }
        catch ( ExecutionException e ) {
            return;
        }
        catch (Exception e) {
            fail("Unexpected exception");
        }

        fail("No exception was thrown despite timeout condition and no task finishing successfully");
    }
    //endregion
}
