import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


public class RunnerTest {
    public Integer exceptionFunc(List<Integer> list) throws ProcessorException {
        throw new ProcessorException("some exception");
    }

    @Test
    public void testThreeProcess() {
        Runner<Integer> a = new RunnerImpl<>();
        AtomicInteger var = new AtomicInteger(0);
        Function<List<Integer>, Integer> func = list -> var.incrementAndGet();
        Set<Processor<Integer>> set = new HashSet<>();
        set.add(new ProcessorImpl<>("one", Collections.emptyList(), func));
        set.add(new ProcessorImpl<>("two", List.of("one"), func));
        set.add(new ProcessorImpl<>("three", List.of("two", "one"), func));
        int maxThreads = 8;
        int maxIterations = 100000;
        try {
            a.runProcessors(set, maxThreads, maxIterations);
        } catch (ProcessorException e) {
            Assert.fail();
        }
        int potentialAns = set.size() * maxIterations;
        Assert.assertEquals(potentialAns, var.get());
    }

    @Test
    public void testOneThreads() {
        Runner<Integer> a = new RunnerImpl<>();
        AtomicInteger var = new AtomicInteger(0);
        Function<List<Integer>, Integer> func = list -> var.incrementAndGet();
        Set<Processor<Integer>> set = new HashSet<>();
        set.add(new ProcessorImpl<>("one", Collections.emptyList(), func));
        int maxThreads = 8;
        int maxIterations = 100000;
        try {
            a.runProcessors(set, maxThreads, maxIterations);
        } catch (ProcessorException e) {
            Assert.fail();
        }
        int potentialAns = set.size() * maxIterations;
        Assert.assertEquals(potentialAns, var.get());
    }

    @Test
    public void testCycle() {
        Runner<Integer> a = new RunnerImpl<>();
        AtomicInteger var = new AtomicInteger(0);
        Function<List<Integer>, Integer> func = list -> var.incrementAndGet();
        Set<Processor<Integer>> set = new HashSet<>();
        set.add(new ProcessorImpl<>("one", List.of("three"), func));
        set.add(new ProcessorImpl<>("two", List.of("one"), func));
        set.add(new ProcessorImpl<>("three", List.of("two"), func));
        int maxThreads = 8;
        int maxIterations = 100000;
        try {
            a.runProcessors(set, maxThreads, maxIterations);
        } catch (ProcessorException e) {
            //Assert.assertTrue(true);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testWithArguments() {
        Runner<Integer> a = new RunnerImpl<>();
        AtomicInteger var = new AtomicInteger(0);
        AtomicInteger var1 = new AtomicInteger(0);
        Set<Processor<Integer>> set = new HashSet<>();
        set.add(new ProcessorImpl<>("one", Collections.emptyList(), (list) -> var.incrementAndGet()));
        set.add(new ProcessorImpl<>("two", Collections.emptyList(), (list) -> var1.incrementAndGet()));
        set.add(new ProcessorImpl<>("three", List.of("one", "two"), (list) -> list.get(0) + list.get(1)));
        int maxThreads = 8;
        int maxIterations = 100000;
        Map<String, List<Integer>> res = new HashMap<>();
        try {
            res = a.runProcessors(set, maxThreads, maxIterations);
        } catch (ProcessorException e) {
            Assert.fail();
        }
        for (int i = 0; i < maxIterations; ++i) {
            Assert.assertEquals(res.get("one").get(i), Integer.valueOf(i + 1));
            Assert.assertEquals(res.get("two").get(i), Integer.valueOf(i + 1));
            Assert.assertEquals(res.get("three").get(i), Integer.valueOf(2 * i + 2));
        }
    }

    @Test
    public void testNull() {
        Runner<Integer> a = new RunnerImpl<>();
        AtomicInteger var = new AtomicInteger(0);
        Set<Processor<Integer>> set = new HashSet<>();
        set.add(new ProcessorImpl<>("one", Collections.emptyList(), (list) -> var.incrementAndGet()));
        set.add(new ProcessorImpl<>("two", Collections.emptyList(), (list) -> var.incrementAndGet()));
        set.add(new ProcessorImpl<>("three", List.of("one", "two"), (list) -> null));
        int maxThreads = 8;
        int maxIterations = 100000;
        Map<String, List<Integer>> res = new HashMap<>();
        try {
            res = a.runProcessors(set, maxThreads, maxIterations);
        } catch (ProcessorException e) {
            Assert.fail();
        }
        Assert.assertTrue(res.get("one").isEmpty());
        Assert.assertTrue(res.get("two").isEmpty());
        Assert.assertTrue(res.get("three").isEmpty());
    }

    @Test
    public void testTrickyNull() {
        Runner<Integer> a = new RunnerImpl<>();
        AtomicInteger var = new AtomicInteger(0), var1 = new AtomicInteger(0);
        Set<Processor<Integer>> set = new HashSet<>();
        set.add(new ProcessorImpl<>("one", Collections.emptyList(), (list) -> {
            if (var.get() >= 5000) return null;
            return var.incrementAndGet();
        }));
        set.add(new ProcessorImpl<>("two", Collections.emptyList(), (list) -> {
            if (var1.get() >= 10000) return null;
            return var1.incrementAndGet();
        }));
        set.add(new ProcessorImpl<>("three", List.of("one", "two"), (list) -> list.get(0) + list.get(1)));
        int maxThreads = 8;
        int maxIterations = 100000;
        Map<String, List<Integer>> res = new HashMap<>();
        try {
            res = a.runProcessors(set, maxThreads, maxIterations);
        } catch (ProcessorException e) {
            Assert.fail();
        }
        Assert.assertEquals(5000, res.get("one").size());
        Assert.assertEquals(5000, res.get("two").size());
        Assert.assertEquals(5000, res.get("three").size());

        for (int i = 0; i < 5000; ++i) {
            Assert.assertEquals(res.get("one").get(i), Integer.valueOf(i + 1));
            Assert.assertEquals(res.get("two").get(i), Integer.valueOf(i + 1));
            Assert.assertEquals(res.get("three").get(i), Integer.valueOf(2 * i + 2));
        }
    }

    @Test
    public void testException() {
        Runner<Integer> a = new RunnerImpl<>();
        Set<Processor<Integer>> set = new HashSet<>();
        set.add(new ProcessorImpl<>("one", Collections.emptyList(), (list) -> 0));
        set.add(new ExceptionProcessorImpl<>("two", List.of("one")));
        int maxThreads = 8;
        int maxIterations = 100000;
        try {
            a.runProcessors(set, maxThreads, maxIterations);
        } catch (ProcessorException e) {
            return;
        }
        Assert.fail();
    }
}
