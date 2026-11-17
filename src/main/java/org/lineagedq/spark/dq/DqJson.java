package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Doc/ghi JSON trong /dq/ qua Hadoop FileSystem API (Java co san Configuration tu SparkSession;
 * phia Python di WebHDFS vi khong co).
 */
public final class DqJson {

    private static final Logger LOG = LoggerFactory.getLogger(DqJson.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DqJson() {
    }

    /** @return null khi file khong ton tai hoac khong doc duoc - KHONG nem exception. */
    public static JsonNode read(Configuration hadoopConf, String hdfsBaseUri, String path) {
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsBaseUri), new Configuration(hadoopConf));
            Path p = new Path(path);
            if (!fs.exists(p)) return null;
            try (FSDataInputStream in = fs.open(p)) {
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                in.transferTo(buf);
                return MAPPER.readTree(buf.toByteArray());
            }
        } catch (Exception e) {  // doc metadata DQ khong duoc lam hong job
            LOG.warn("=== DqJson: khong doc duoc {}: {}", path, e.toString());
            return null;
        }
    }

    /** @return true neu ghi duoc. Ghi de neu da co. */
    public static boolean write(Configuration hadoopConf, String hdfsBaseUri, String path,
                                byte[] bytes) {
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsBaseUri), new Configuration(hadoopConf));
            try (FSDataOutputStream out = fs.create(new Path(path), true)) {
                out.write(bytes);
            }
            return true;
        } catch (Exception e) {
            LOG.warn("=== DqJson: khong ghi duoc {}: {}", path, e.toString());
            return false;
        }
    }

    public static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
