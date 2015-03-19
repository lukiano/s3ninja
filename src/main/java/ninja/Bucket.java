/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.cache.ValueComputer;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Represents a bucket.
 * <p>
 * Internally a bucket is just a directory within the base directory.
 * </p>
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
 */
public class Bucket {

    private final Path path;
    private static final Cache<String, Boolean> publicAccessCache = CacheManager.createCache("public-bucket-access");


    /**
     * Creates a new bucket based on the given directory.
     *
     * @param path the directory which stores the contents of the bucket.
     */
    public Bucket(Path path) {
        this.path = path;
    }

    /**
     * Returns the name of the bucket.
     *
     * @return the name of the bucket
     */
    public String getName() {
        return path.getFileName().toString();
    }

    /**
     * Deletes the bucket and all of its contents.
     */
    public void delete() {
      try {
        removeRecursive(path);
      } catch (IOException e) {
        throw Exceptions.handle(Storage.LOG, e);
      }
    }

  public static void removeRecursive(Path path) throws IOException
  {
    Files.walkFileTree(path, new SimpleFileVisitor<Path>()
    {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException
      {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
      {
        // try to delete the file anyway, even if its attributes
        // could not be read, since delete-only access is
        // theoretically possible
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
      {
        if (exc == null)
        {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
        else
        {
          // directory iteration failed; propagate exception
          throw exc;
        }
      }
    });
  }

    /**
     * Creates the bucket.
     * <p>
     * If the underlying directory already exists, nothing happens.
     * </p>
     */
    public void create() {
        if (Files.notExists(path)) {
          try {
            Files.createDirectories(path);
          } catch (IOException e) {
            throw Exceptions.handle(Storage.LOG, e);
          }
        }
    }

      /**
       * Returns a list of all stored objects
       *
       * @return a list of all objects in the bucket.
       */
      public List<StoredObject> getObjects() {
        try (DirectoryStream<Path> ds =
                     Files.newDirectoryStream(path, f -> Files.isRegularFile(f) && !isMetadata(f))) {
          return StreamSupport.stream(ds.spliterator(), false).map(StoredObject::new).collect(Collectors.toList());
        } catch (IOException e) {
          return Collections.emptyList();
        }
      }

      private boolean isMetadata(Path path) {
        return path.getFileName().toString().startsWith("__");
      }

    /**
     * Determines if the bucket is private or public accessible
     *
     * @return <tt>true</tt> if the bucket is public accessible, <tt>false</tt> otherwise
     */
    public boolean isPrivate() {
      Boolean result = publicAccessCache.get(getName(), new ValueComputer<String, Boolean>() {
        @Nullable
        @Override
        public Boolean compute(@Nonnull String key) {
          return Files.exists(getPublicMarkerFile());
        }
      });
      return result != null && !result;
    }

    private Path getPublicMarkerFile() {
        return path.resolve("__ninja_public");
    }

    /**
     * Marks the bucket as private accessible.
     */
    public void makePrivate() {
        if (Files.exists(getPublicMarkerFile())) {
          try {
            Files.delete(getPublicMarkerFile());
          } catch (IOException e) {
            throw Exceptions.handle(Storage.LOG, e);
          }
          publicAccessCache.put(getName(), false);
        }
    }

    /**
     * Marks the bucket as public accessible.
     */
    public void makePublic() {
        if (!Files.exists(getPublicMarkerFile())) {
            try {
                Files.createFile(getPublicMarkerFile());
            } catch (IOException e) {
                throw Exceptions.handle(Storage.LOG, e);
            }
        }
        publicAccessCache.put(getName(), true);
    }

    /**
     * Returns the underlying directory as File.
     *
     * @return a <tt>File</tt> representing the underlying directory
     */
    public Path getPath() {
        return path;
    }

    /**
     * Determines if the bucket exists.
     *
     * @return <tt>true</tt> if the bucket exists, <tt>false</tt> otherwise
     */
    public boolean exists() {
        return Files.exists(path);
    }

    /**
     * Returns the child object with the given id.
     *
     * @param id the name of the requested child object. Must not contain .. / or \
     * @return the object with the given id, might not exist, but is always non null
     */
    public StoredObject getObject(String id) {
        if (id.contains("..") || id.contains("/") || id.contains("\\")) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage(
                                    "Invalid object name: %s. A object name must not contain '..', '/' or '\\'",
                                    id)
                            .handle();
        }
      Path object = path.resolve(id);
      return new StoredObject(object);
    }
}
