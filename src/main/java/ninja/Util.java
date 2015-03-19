package ninja;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import java.io.IOException;
import java.nio.file.Path;

public final class Util {

    private Util() {}

    public static MD5 md5(Path path) throws IOException {
        return new MD5(com.google.common.io.Files.hash(path.toFile(), Hashing.md5()));
    }

    public static final class MD5 {
        private final HashCode hash;

        public MD5(HashCode hashCode) {
            hash = hashCode;
        }

        public String base64() {
            return BaseEncoding.base64().encode(hash.asBytes());
        }

        public String etag() {
            return "\"" + hash + "\"";
        }
    }
}
