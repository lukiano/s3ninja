/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents a stored object.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
 */
public class StoredObject {
    private final Path path;

    /**
     * Creates a new StoredObject based on a file.
     *
     * @param path the contents of the object.
     */
    public StoredObject(Path path) {
        this.path = path;
    }

    /**
     * Returns the name of the object.
     *
     * @return the name of the object
     */
    public String getName() {
        return path.getFileName().toString();
    }

    /**
     * Returns the size of the object.
     *
     * @return a string representation of the byte-size of the object
     */
    public String getSize() {
        try {
            return NLS.formatSize(Files.size(path));
        } catch (IOException e) {
            throw Exceptions.handle(Storage.LOG, e);
        }
    }

    /**
     * Returns the last modified date of the object
     *
     * @return a string representation of the last modification date
     */
    public String getLastModified() {
        return NLS.toUserString(getLastModifiedInstant());
    }

    public Instant getLastModifiedInstant() {
        try {
            return Files.getLastModifiedTime(path).toInstant();
        } catch (IOException e) {
            throw Exceptions.handle(Storage.LOG, e);
        }
    }

    /**
     * Deletes the object
     */
    public void delete() {
      try {
        Files.delete(path);
        Files.delete(getPropertiesPath());
      } catch (IOException e) {
        throw Exceptions.handle(Storage.LOG, e);
      }
    }

    /**
     * Returns the underlying file
     *
     * @return the underlying file containing the stored contents
     */
    public Path getPath() {
        return path;
    }

    /**
     * Determines if the object exists
     *
     * @return <tt>true</tt> if the object exists, <tt>false</tt> otherwise
     */
    public boolean exists() {
        return Files.exists(path);
    }

    /**
     * Returns all properties stored along with the object.
     * <p>
     * This is the Content-MD5, Content-Type and any x-amz-meta- header.
     * </p>
     *
     * @return a set of name value pairs representing all properties stored for this object
     * @throws Exception in case of an IO error
     */
    public Map<String, String> getProperties() throws Exception {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(getPropertiesPath())) {
          props.load(in);
        }
        return props.stringPropertyNames().stream().collect(Collectors.toMap(Function.<String>identity(), props::getProperty));
    }

    /**
     * Returns the file used to store the properties and meta headers.
     *
     * @return the underlying file used to store the meta infos
     */
    public Path getPropertiesPath() {
        return path.resolveSibling("__ninja_" + path.getFileName().toString() + ".properties");
    }

    /**
     * Stores the given meta infos for the stored object.
     *
     * @param properties properties to store
     * @throws IOException in case of an IO error
     */
    public void storeProperties(Map<String, String> properties) throws IOException {
        Properties props = new Properties();
        props.putAll(properties);
        try (OutputStream out = Files.newOutputStream(getPropertiesPath())) {
          props.store(out, "");
        }
    }
}
