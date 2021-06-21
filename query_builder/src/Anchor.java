import java.util.Map;

public class Anchor {
    public String name = "";
    public String shortName = "";
    public boolean isCondition;
    public boolean multiLine;
    public String firstRel = "";
    public String secondRel = "";

    public Anchor(String name) {
        this.name = name;

        String[] keyWordParts = name.split("_");
        int length = keyWordParts.length;
        this.shortName = keyWordParts[0];
        if (length > 1) {
            this.isCondition = keyWordParts[1].equals("condition");
            this.multiLine = (this.isCondition || !keyWordParts[1].equals("singleLine"));
            if (length > 2 ){
                firstRel = keyWordParts[2];
            }
            if (length > 3 ){
                secondRel = keyWordParts[3];
            }
        }
    }

}
