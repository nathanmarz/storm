package backtype.storm.nimbus;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

public interface IStorage {
    /**
     * throws Exception if path already exists.
     * @param dir director path to be created.
     * @throws IOException
     */
    void mkdir(Path dir) throws IOException;

    /**
     * throws Exception if path already exists.
     * @param file path to create
     * @return output stream that can be used to write.
     */
    OutputStream create(Path file) throws IOException;

    /**
     * list of files under a directory, throws Exception if
     * path does not exist.
     */
    List<Path> listFiles(Path dir) throws IOException;

    /**
     *
     * @param path path to be deleted
     * @param recursive if true and path is a directory everything under it will be deleted.
     * @throws IOException
     */
    void delete(Path path, boolean recursive) throws IOException;

    /**
     * move src to dest, throws exception if dest already exists.
     * @throws IOException
     */
    void move(Path src, Path dest) throws IOException;

    /**
     * copy src to dest, throws exception if dest already exists.
     */
    void copy(Path src, Path dest) throws IOException;

    /**
     * returns true if a path exists.
     */
    boolean exists(Path path) throws IOException;

    /**
     * returns true if path is directory, throws exception if it does not exist.
     */
    boolean isDir(Path path) throws IOException;
}
