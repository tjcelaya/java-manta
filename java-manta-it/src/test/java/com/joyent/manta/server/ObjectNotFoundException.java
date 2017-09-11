package com.joyent.manta.server;

import java.io.IOException;
import java.nio.file.Path;

public class ObjectNotFoundException extends IOException {

    private final String path;

    private final boolean isDirectory;

    public ObjectNotFoundException(final Path path) {
        this(path, false);
    }

    public ObjectNotFoundException(final Path path, final boolean isDirectory) {
        this.path = path.toString();
        this.isDirectory = isDirectory;
    }

    public String getPath() {
        return path;
    }

    public boolean isDirectory() {
        return isDirectory;
    }
}
