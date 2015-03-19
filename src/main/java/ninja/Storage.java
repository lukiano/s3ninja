/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.Sirius;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.nls.NLS;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Storage service which takes care of organizing buckets on disk.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
 */
@Register(classes = Storage.class)
public class Storage {

    private Path baseDir;
    protected static final Log LOG = Log.get("storage");

    @ConfigValue("storage.awsAccessKey")
    private String awsAccessKey;

    @ConfigValue("storage.awsSecretKey")
    private String awsSecretKey;

    @ConfigValue("storage.autocreateBuckets")
    private boolean autocreateBuckets;

    protected Path getBaseDir() {
        baseDir = getBaseDirUnchecked();

        if (!Files.exists(baseDir)) {
            throw Exceptions.handle()
                            .to(LOG)
                            .withSystemErrorMessage("Basedir '%s' does not exist!", baseDir.toAbsolutePath())
                            .handle();
        }
        if (!Files.isDirectory(baseDir)) {
            throw Exceptions.handle()
                            .to(LOG)
                            .withSystemErrorMessage("Basedir '%s' is not a directory!", baseDir.toAbsolutePath())
                            .handle();
        }

        return baseDir;
    }

    private Path getBaseDirUnchecked() {
        if (baseDir == null) {
            if (Sirius.isStartedAsTest()) {
                baseDir = Paths.get(System.getProperty("java.io.tmpdir"), "s3ninja_test");
              try {
                Files.createDirectories(baseDir);
              } catch (IOException e) {
                throw Exceptions.handle(Storage.LOG, e);
              }
            } else {
                baseDir = Paths.get(Sirius.getConfig().getString("storage.baseDir"));
            }
        }

        return baseDir;
    }

    /**
     * Returns the base directory as string.
     *
     * @return a string containing the path of the base directory. Will contain additional infos, if the path is
     * not usable
     */
    public String getBasePath() {
        StringBuilder sb = new StringBuilder(getBaseDirUnchecked().toString());
        if (!Files.exists(getBaseDirUnchecked())) {
            sb.append(" (non-existent!)");
        } else if (!Files.isDirectory(getBaseDirUnchecked())) {
            sb.append(" (no directory!)");
        } else {
            sb.append(" (Free: ");
            sb.append(NLS.formatSize(getFreeSpace(getBaseDir())));
            sb.append(")");
        }

        return sb.toString();
    }

    public long getFreeSpace(Path path) {
      try {
        return Files.getFileStore(path).getUsableSpace();
      } catch (IOException e) {
        throw Exceptions.handle(Storage.LOG, e);
      }
    }

    /**
     * Enumerates all known buckets.
     *
     * @return a list of all known buckets
     */
    public List<Bucket> getBuckets() {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(getBaseDir(), Files::isDirectory)) {
          return StreamSupport.stream(ds.spliterator(), false).map(Bucket::new).collect(Collectors.toList());
        } catch (IOException e) {
          return Collections.emptyList();
        }
    }

    /**
     * Returns a bucket with the given name
     *
     * @param bucket the name of the bucket to fetch. Must not contain .. or / or \
     * @return the bucket with the given id. Might not exist, but will never be <tt>null</tt>
     */
    public Bucket getBucket(String bucket) {
        if (bucket.contains("..") || bucket.contains("/") || bucket.contains("\\")) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage(
                                    "Invalid bucket name: %s. A bucket name must not contain '..' '/' or '\\'",
                                    bucket)
                            .handle();
        }
        return new Bucket(getBaseDir().resolve(bucket));
    }

    /**
     * Returns the used AWS access key.
     *
     * @return the AWS access key
     */
    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    /**
     * Returns the AWS secret key used to verify hashes.
     *
     * @return the AWS secret key
     */
    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    /**
     * Determines if buckets should be automatically created.
     *
     * @return <tt>true</tt> if buckets can be auto-created upon the first request
     */
    public boolean isAutocreateBuckets() {
        return autocreateBuckets;
    }
}
