package org.ajur.demo.kstreams.giigaspaces.store.gks;

public class SpacePropertyDescriptor {

    private Class type;
    private String name;
    private boolean indexed;

    public SpacePropertyDescriptor( String name, Class type, boolean indexed) {

        this.type = type;
        this.name = name;
        this.indexed = indexed;
    }

    public Class getType() {
        return type;
    }

    public void setType(Class type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }

    @Override
    public String toString() {
        return "SpacePropertyDescriptor{" +
                "type=" + type +
                ", name='" + name + '\'' +
                ", indexed=" + indexed +
                '}';
    }
}
