import java.util.ArrayList;
import java.util.List;

class ExceptionProcessorImpl<T> implements Processor<T> {

    public ExceptionProcessorImpl(final String id, final List<String> inputIds) {
        this.id = id;
        this.inputs = new ArrayList<String>(inputIds);
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
        throw new ProcessorException("you got an exception!");
    }

    private List<String> inputs;
    private String id;
}
