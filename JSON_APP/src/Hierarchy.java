import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class HTree<T extends Element<R>, R extends Entity> {

    private final List<T> all;

    public HTree(List<T> all) {
        this.all = all;
    }

    public synchronized T getById(R id) {
        return this.all.stream()
                .filter(element -> element.checkIfEquals(id))
                .findFirst()
                .orElseThrow(RuntimeException::new);
    }

    private synchronized Stream<T> getParentsStream(T element) {
        return element.getParentIds().map(this::getById);
    }
    public synchronized List<T> getParents(T element) {
        return this.getParentsStream(element).collect(Collectors.toList());
    }
    public synchronized boolean isRoot(T element) {
        return !this.getParentsStream(element).findFirst().isPresent();
    }
    private synchronized Stream<T> getRootsStream() {
        return this.all.stream()
                .filter(this::isRoot);
    }
    public synchronized List<T> getRoots() {
        return getRootsStream().collect(Collectors.toList());
    }

    private synchronized Stream<T> getChildrenStream(T element) {
        return this.all.stream()
                .filter(elt -> this.getParentsStream(elt).filter(elt2 -> elt2.checkIfEquals(element.getId())).findFirst().isPresent());
    }
    public synchronized List<T> getChildren(T element) {
        return this.getChildrenStream(element)
                .collect(Collectors.toList());
    }
    public synchronized boolean isLeaf(T element) {
        return !this.getChildrenStream(element).findFirst().isPresent();
    }
    private synchronized Stream<T> getLeavesStream() {
        return this.all.stream()
                .filter(this::isLeaf);
    }
    public synchronized List<T> getLeaves() {
        return getLeavesStream().collect(Collectors.toList());
    }

}

/*public class Main {
    public static void main(String args[]) throws IOException {
        List<Element<String>> hList = new ArrayList<>();
        Path path = Paths.get("src.csv");

        Files.lines(path).forEach(p -> {String[] s = p.split(",");
            hList.add(new Element<String>(s[0], s.length == 1 ? "" : s[1]));});

        HTree<Element<String>, String> hTree = new HTree<>(hList);
        System.out.println(hList.stream()
                .filter(p -> hTree.isLeaf(p))
                .map(Element::getId)
                .collect(Collectors.joining(",")));

        hList.stream()
                .map(Element::getId)
                .collect(Collectors.groupingBy(p -> {char[] arr = p.toUpperCase().toCharArray();
                    Arrays.sort(arr);
                    return new String(arr);}))
                .values()
                .stream()
                .filter(p -> p.size() > 1)
                .map(p -> p.stream().collect(Collectors.joining(",")))
                .forEach(System.out::println);
    }
}*/
