import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class RunnerImpl<T> implements Runner<T> {

    private final List<Thread> workingThreads = new ArrayList<>();
    private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();

    private void setThreadsArray(int maxThreads) {
        Runnable worker = () -> {
            try {
                while (!Thread.interrupted()) {
                    tasks.take().run();
                }
            } catch (InterruptedException ignored) {
                // ignore
            } finally {
                Thread.currentThread().interrupt();
            }
        };

        for (int i = 0; i < maxThreads; ++i) {
            workingThreads.add(new Thread(worker));
        }
        workingThreads.forEach(Thread::start);
    }

    private boolean dfs(String vertex, Map<String, Integer> used, Map<String, List<String>> edges) {
        used.replace(vertex, 1);
        boolean hasCycle = false;
        for (String son : edges.get(vertex)) {
            if (used.get(son).equals(1)) return true;
            hasCycle |= dfs(son, used, edges);
        }
        used.replace(vertex, 2);
        return hasCycle;
    }

    private boolean checkCycles(Set<Processor<T>> processors) {
        // TODO rewrite this shit
        Map<String, List<String>> edges = new HashMap<>();
        for (Processor<T> processor : processors) {
            edges.put(processor.getId(), new ArrayList<>());
        }
        for (Processor<T> processor : processors) {
            List<String> ancestors = processor.getInputIds();
            for (String a : ancestors) {
                edges.get(a).add(processor.getId());
            }
        }
        Map<String, Integer> used = new HashMap<>();
        for (String key : edges.keySet()) {
            used.put(key, 0);
        }
        boolean hasCycle = false;
        for (String key : used.keySet()) {
            if (used.get(key).equals(0)) {
                hasCycle |= dfs(key, used, edges);
            }
        }
        return hasCycle;
    }

    private void close() {
        workingThreads.forEach(Thread::interrupt);

        for (Thread thread : workingThreads) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {
                // ignore
            }
        }
    }

    class ProcessorsInfo {
        private final Map<String, AtomicReferenceArray<T>> results;
        private final Map<String, Lock> processorLocks;
        private final Map<String, AtomicInteger> numberOfDoneIterations;
        private final Map<String, Processor<T>> idToProcessor;
        private final int maxIterations;
        private final AtomicInteger nullIteration;
        private final AtomicReference<ProcessorException> exception;

        public ProcessorsInfo(Set<Processor<T>> processors, int maxIterations) {
            results = new HashMap<>();
            processorLocks = new HashMap<>();
            numberOfDoneIterations = new HashMap<>();
            nullIteration = new AtomicInteger(maxIterations);
            idToProcessor = new HashMap<>();
            exception = new AtomicReference<>(null);
            for (Processor<T> processor : processors) {
                results.put(processor.getId(), new AtomicReferenceArray<>(maxIterations));
                processorLocks.put(processor.getId(), new ReentrantLock());
                numberOfDoneIterations.put(processor.getId(), new AtomicInteger(0));
                idToProcessor.put(processor.getId(), processor);
            }
            this.maxIterations = maxIterations;
        }

        public Map<String, List<T>> getFinalResults() throws ProcessorException {
            if (checkException()) throw exception.get();
            Map<String, List<T>> finalResults = new HashMap<>();
            for (String key : results.keySet()) {
                finalResults.put(key, new ArrayList<>());
                List<T> dst = finalResults.get(key);
                AtomicReferenceArray<T> src = results.get(key);
                for (int i = 0; i < nullIteration.get(); ++i) {
                    dst.add(src.get(i));
                }
            }
            return finalResults;
        }

        public Lock getLocker(String procId) {
            return processorLocks.get(procId);
        }

        private int getNumberOfDoneIterations(String procId) {
            return numberOfDoneIterations.get(procId).get();
        }

        private int getMaxIterations() {
            return maxIterations;
        }

        private int getIterationWithNull() {
            return nullIteration.get();
        }

        private void setNullIteration(int newVal) {
            int prevVal = nullIteration.get();
            while (prevVal > newVal) {
                nullIteration.compareAndSet(prevVal, newVal);
                prevVal = nullIteration.get();
            }
        }

        public boolean checkIfComplete() {
            if (exception.get() != null) return true;
            for (AtomicInteger num : numberOfDoneIterations.values()) {
                if (num.get() < nullIteration.get()) return false;
            }
            return true;
        }

        public boolean checkException() {
            return !(exception.get() == null);
        }

        public void setException(ProcessorException e) {
            exception.set(e);
        }

        public void process(String id, int iteration) throws ProcessorException {
            Processor<T> processor = idToProcessor.get(id);
            List<T> inputs = new ArrayList<>();
            for (String ancestor : processor.getInputIds()) {
                inputs.add(results.get(ancestor).get(iteration));
            }
            T res = processor.process(inputs);
            if (res == null) {
                setNullIteration(iteration);
            }
            results.get(id).set(iteration, res);
            numberOfDoneIterations.get(id).incrementAndGet();
        }

        private List<String> getAncestors(String procId) {
            return idToProcessor.get(procId).getInputIds();
        }

    }

    private boolean canProcess(ProcessorsInfo processorsInfo, String id, int iteration) {
        if (processorsInfo.getNumberOfDoneIterations(id) != iteration) return false;
        for (String ancestor : processorsInfo.getAncestors(id)) {
            if (processorsInfo.getNumberOfDoneIterations(ancestor) <= iteration) return false;
        }
        return true;
    }

    private boolean shouldStop(ProcessorsInfo processorsInfo, int iteration) {
        return processorsInfo.getIterationWithNull() <= iteration || processorsInfo.checkException();
    }

    private void addTaskToQueue(ProcessorsInfo processorsInfo, String id, int iteration) {
        tasks.add(() -> {
            Lock locker = processorsInfo.getLocker(id);
            locker.lock();
            if (!shouldStop(processorsInfo, iteration)) {
                if (canProcess(processorsInfo, id, iteration)) {
                    try {
                        processorsInfo.process(id, iteration);
                        if (iteration + 1 < processorsInfo.getMaxIterations()) {
                            addTaskToQueue(processorsInfo, id, iteration + 1);
                        }
                    } catch (ProcessorException e) {
                        processorsInfo.setException(e);
                    }
                } else {
                    addTaskToQueue(processorsInfo, id, iteration);
                }
            }
            locker.unlock();
        });
    }

    @Override
    public Map<String, List<T>> runProcessors(Set<Processor<T>> processors, int maxThreads, int maxIterations) throws ProcessorException {
        if (checkCycles(processors)) {
            throw new ProcessorException("I found a cycle!");
        }

        setThreadsArray(maxThreads);

        ProcessorsInfo processorInfo = new ProcessorsInfo(processors, maxIterations);
        for (Processor<T> processor : processors) {
            addTaskToQueue(processorInfo, processor.getId(), 0);
        }

        while (!processorInfo.checkIfComplete()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        close();
        return processorInfo.getFinalResults();
    }
}