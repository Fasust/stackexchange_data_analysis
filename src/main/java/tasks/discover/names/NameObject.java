package tasks.discover.names;

public class NameObject implements Comparable<NameObject> {

    private Integer nameCount;
    private String name;

    public NameObject(Integer nameCount, String name) {
        this.nameCount = nameCount;
        this.name = name;
    }

    @Override
    public int compareTo(NameObject o) {
        if(this.getNameCount().equals(o.getNameCount())) return 1;
        if(this.getNameCount() > o.getNameCount()) return -1;
        return 1;
    }

    public Integer getNameCount() {
        return nameCount;
    }

    public String getName() {
        return name;
    }
}
