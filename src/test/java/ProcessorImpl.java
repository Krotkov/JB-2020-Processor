import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ProcessorImpl<T> implements Processor<T> {
    Function<List<T>, T> function;

    public ProcessorImpl(final String id, final List<String> inputIds, Function<List<T>, T> function) {
        this.id = id;
        this.inputs = new ArrayList<String>(inputIds);
        this.function = function;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public List<String> getInputIds() {
        return new ArrayList<>(inputs);
    }

    @Override
    public T process(List<T> input) throws ProcessorException {
        return function.apply(input);
    }

    private List<String> inputs;
    private String id;
}
