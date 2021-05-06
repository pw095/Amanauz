import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class FlowTree {

    class FlowDependency {
        private int elementId;
        private int childElementId;
        private int elementLevel;
    }

    private List<FlowDependency> flowDependencyList = new ArrayList<>();

    public void addDependency(FlowDependency flowDependency) {
        flowDependencyList.add(flowDependency);
    }
}
