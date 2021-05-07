import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

class Element<R extends Entity> implements Callable<R> {

    private R id;
    private List<R> parentIds = new ArrayList<>();

    Element(R id, R ... parentIds) {
        this.id = id;
        for(R parentId : parentIds) {
            this.parentIds.add(parentId);
        }
    }

    public R getId() {
        return id;
    }

    public Stream<R> getParentIds() {
        return parentIds.stream();
    }

    public boolean checkIfEquals(R id) {
        return this.getId().equals(id);
    }

    public R call() {
        id.entityLoad();
        return id;
    }
    @Override
    public boolean equals(Object obj) {
        return id.equals(obj);
    }
}