package org.apache.storm.flux.model;

/**
 * Represents an include. Includes can be either a file or a classpath resource.
 *
 * If an include is marked as `override=true` then existing properties will be replaced.
 *
 */
public class IncludeDef {
    private boolean resource = false;
    boolean override = false;
    private String file;

    public boolean isResource() {
        return resource;
    }

    public void setResource(boolean resource) {
        this.resource = resource;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public boolean isOverride() {
        return override;
    }

    public void setOverride(boolean override) {
        this.override = override;
    }
}
