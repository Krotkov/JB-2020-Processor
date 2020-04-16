import java.util.List;

public interface Processor<T> {



    /**

     * @return processor id, immutable, unique among all instances, not null

     */

    String getId();



    /**

     * @return a list of processors that have to be executed before this one

     *         and whose results must be passed to main.Processor::process,

     *         immutable, can be null or empty, both means no inputs

     */

    List<String> getInputIds();



    /**

     * @param input outputs of the processors whose ids are returned by main.Processor::getInputIds, not null, but can be empty

     * @return output of the processing, null if no output is produced

     * @throws ProcessorException if error occurs during processing

     */

    T process(List<T> input) throws ProcessorException;

}

