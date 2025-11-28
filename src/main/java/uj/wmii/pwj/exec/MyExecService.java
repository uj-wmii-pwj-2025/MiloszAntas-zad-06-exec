package uj.wmii.pwj.exec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MyExecService implements ExecutorService {
    private static final int DEFAULT_VALUE = 4;

    private class InnerThread extends Thread {
        @Override
        public void run() {
            try {
                while (true) {
                    if (executorState == MyExecService.State.SHUTDOWN && taskQueue.isEmpty()) break;

                    TaskTuple<?> task = taskQueue.take();
                    if (task.canRun())
                        task.run();
                }
            }
            catch (Exception e) {
                // do nothing I think
            }
            finally {
                synchronized (threadLock) {
                    if ( (--threadCount) == 0 ) {
                        threadLock.notifyAll();
                    }
                }
            }
        }
    }

    private class TaskTuple<T> implements Runnable {
        CompletableFuture<T> future;
        Callable<T> callable;
        boolean returnSpecific;
        T returnValue;

        TaskTuple(Callable<T> setCallable, CompletableFuture<T> setFuture) {
            future = setFuture;
            callable = setCallable;
            returnSpecific = false;
        }

        TaskTuple(Runnable setTask, CompletableFuture<T> setFuture, T setReturnValue) {
            future = setFuture;
            returnSpecific = true;
            returnValue = setReturnValue;

            Callable<T> setCallable = () -> {
                setTask.run();
                return null;
            };
            callable = setCallable;
        }

        public boolean canRun() {
            return !future.isCancelled();
        }

        @Override
        public void run() {
            if (future.isCancelled()) return;

            try {
                T result = callable.call();
                if ( returnSpecific ) future.complete(returnValue);
                else future.complete(result);
            }
            catch (Exception e) {
                future.completeExceptionally(e);
            }
        }
    }

    private BlockingDeque<TaskTuple<?>> taskQueue;
    private List<InnerThread> threads;
    private Object threadLock;
    private int threadCount;
    private enum State {
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }
    private volatile State executorState;

    //region Initializer
    private MyExecService(int threadCount) {
        executorState = State.RUNNING;
        taskQueue = new LinkedBlockingDeque<TaskTuple<?>>();
        this.threadCount = threadCount;
        threads = new ArrayList<>();
        threadLock = new Object();

        for ( int i = 0; i < threadCount; i++ ) {
            InnerThread a = new InnerThread();
            threads.add(a);
            a.start();
        }
    }
    static MyExecService newInstance() {
        return new MyExecService(DEFAULT_VALUE);
    }
    static MyExecService newInstance(int setThreadCount) {
        if ( setThreadCount <= 0 ) throw new IllegalArgumentException();
        return new MyExecService(setThreadCount);
    }
    //endregion

    //region Control functions
    @Override
    public void shutdown() {
        this.executorState = State.SHUTDOWN;
    }

    @Override
    public List<Runnable> shutdownNow() {
        this.executorState = State.SHUTDOWN;
        List<Runnable> awaiting = new ArrayList<>();
        taskQueue.drainTo(awaiting);

        for (Thread runner : threads) {
            runner.interrupt();
        }
        return awaiting;
    }

    @Override
    public boolean isShutdown() {
        return executorState == State.SHUTDOWN;
    }

    @Override
    public boolean isTerminated() {
        if ( this.executorState == State.SHUTDOWN && taskQueue.isEmpty() ) this.executorState = State.TERMINATED;
        return executorState == State.TERMINATED;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long duration = unit.MINUTES.toMillis(timeout);
        long endTime = System.currentTimeMillis() + duration;

        synchronized (threadLock) {
            while ( threadCount > 0 ) {
                long remainingTime = endTime - System.currentTimeMillis();

                if ( remainingTime <= 0 ) {
                    return false;
                }
                threadLock.wait(remainingTime);
            }
        }
        return true;
    }
    //endregion

    //region Submit Functions
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        if ( task == null ) throw new NullPointerException();
        if ( executorState != State.RUNNING ) throw new RejectedExecutionException();

        CompletableFuture<T> future = new CompletableFuture<>();
        TaskTuple<T> fullTask = new TaskTuple<>(task, future);

        taskQueue.offer(fullTask);

        return future;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        if ( task == null ) throw new NullPointerException();
        if ( executorState != State.RUNNING ) throw new RejectedExecutionException();

        CompletableFuture<T> future = new CompletableFuture<>();
        TaskTuple<T> fullTask = new TaskTuple<>(task, future, result);

        taskQueue.offer(fullTask);

        return future;
    }

    @Override
    public Future<?> submit(Runnable task) {
        if ( task == null ) throw new NullPointerException();
        if ( executorState != State.RUNNING ) throw new RejectedExecutionException();

        CompletableFuture<Object> future = new CompletableFuture<>();
        TaskTuple<Object> fullTask = new TaskTuple<>(task, future, null);

        taskQueue.offer(fullTask);

        return future;
    }
    //endregion

    //region InvokeAll functions
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        if ( tasks == null ) throw new NullPointerException();
        return invokeAllHelper(tasks);
    }

    private <R extends Callable<T>, T> List<Future<T>> invokeAllHelper(Collection<R> tasks) throws InterruptedException {
        List<Future<T>> result = new ArrayList<>();
        for (R task : tasks) {
            result.add(this.submit(task));
        }

        for (Future<T> await : result) {
            try {
                await.get();
            }
            catch (InterruptedException e) {
                for (Future<T> task : result) {
                    task.cancel(true);
                }
                throw e;
            }
            catch (ExecutionException e) {
                throw new InterruptedException();
            }
            catch (Exception e) {
                // ignore other exceptions
            }
        }
        return result;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        if ( tasks == null ) throw new NullPointerException();
        return invokeAllHelper(tasks, timeout, unit);
    }

    private <R extends Callable<T>, T> List<Future<T>> invokeAllHelper(Collection<R> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        long duration = unit.toNanos(timeout);
        long endTime = System.nanoTime() + duration;

        List<Future<T>> result = new ArrayList<>();
        for (R task : tasks) {
            result.add(this.submit(task));
        }

        for (Future<T> await : result) {
            if ( !await.isDone() ) try {
                long remainingTime = endTime - System.nanoTime();

                if ( remainingTime <= 0 )
                    await.cancel(true);
                else
                    await.get(remainingTime, TimeUnit.NANOSECONDS);
            }
            catch (TimeoutException e) {
                await.cancel(true);
            }
            catch (InterruptedException e) {
                for (Future<T> task : result) {
                    task.cancel(true);
                }
                throw e;
            }
            catch (ExecutionException e) {
                throw new InterruptedException();
            }
            catch (Exception e) {
                // ignore other exceptions
            }
        }
        return result;
    }
    //endregion

    //region InvokeAny functions
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        if ( tasks == null ) throw new NullPointerException();
        if ( tasks.isEmpty() ) throw new IllegalArgumentException();
        return invokeAnyHelper(tasks);
    }

    private <R extends Callable<T>, T> T invokeAnyHelper(Collection<R> tasks) throws InterruptedException, ExecutionException {
        CountDownLatch await = new CountDownLatch(1);
        AtomicReference<T> value = new AtomicReference<>(null);
        AtomicInteger running = new AtomicInteger(tasks.size());

        List<CompletableFuture<T>> result = new ArrayList<>();
        for (R task : tasks) {
            CompletableFuture<T> future = (CompletableFuture<T>) this.submit(task);

            future.whenComplete((returned, exception) -> {
                if ( exception == null ) {
                    if (value.compareAndSet(null, returned)) {
                        await.countDown();
                    }
                } else {
                    if ( running.decrementAndGet() == 0 ) {
                        await.countDown();
                    }
                }
            });

            result.add(future);
        }

        await.await();

        for ( CompletableFuture<T> f : result ) f.cancel(true);

        T firstCompleted = value.get();
        if ( firstCompleted != null ) return firstCompleted;

        throw new ExecutionException(null);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return invokeAnyHelper(tasks, timeout, unit);
    }

    private <R extends Callable<T>, T> T invokeAnyHelper(Collection<R> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        CountDownLatch await = new CountDownLatch(1);
        AtomicReference<T> value = new AtomicReference<>(null);
        AtomicInteger running = new AtomicInteger(tasks.size());

        List<CompletableFuture<T>> result = new ArrayList<>();
        for (R task : tasks) {
            CompletableFuture<T> future = (CompletableFuture<T>) this.submit(task);

            future.whenComplete((returned, exception) -> {
                if ( exception == null ) {
                    if (value.compareAndSet(null, returned)) {
                        await.countDown();
                    }
                } else {
                    if ( running.decrementAndGet() == 0 ) {
                        await.countDown();
                    }
                }
            });

            result.add(future);
        }

        boolean timedOut = await.await(timeout, unit);

        for ( CompletableFuture<T> f : result ) f.cancel(true);

        if ( !timedOut ) throw new TimeoutException();

        T firstCompleted = value.get();
        if ( firstCompleted != null ) return firstCompleted;

        throw new ExecutionException(null);
    }
    //endregion
    @Override
    public void execute(Runnable command) {
        if ( command == null )
            throw new NullPointerException();
        if ( this.executorState == State.RUNNING )
            taskQueue.offer(new TaskTuple<Object>(command, new CompletableFuture<>(), null));
    }
}
