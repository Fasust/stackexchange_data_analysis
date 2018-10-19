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
        /**
         * We are returning one when the comparison is equal even though the documentation says we should return 0.
         * We made it like this because if not, all the names with the same number of appearances (e.g. 1) wont be included
         * in the set, as it seems that internally the data structure doesn't add values if they are equal when comparing them.
         * This makes sense since if we already know that a name with the same number of appearances is already in the set, we
         * can safely add another one without breaking the data integrity.
         */
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
