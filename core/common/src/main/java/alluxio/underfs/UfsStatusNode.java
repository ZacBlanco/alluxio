package alluxio.underfs;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;

/**
 * The representation of an inode from a UfsStatus
 */
@ThreadSafe
public class UfsStatusNode {
  private final UfsStatus mUfsStatus;
  private final HashMap<String, UfsStatusNode> mChildDirs = new HashMap<>();
  private final HashMap<String, UfsStatusNode> mChildFiles = new HashMap<>();

  /**
   * Create a new instance of {@link UfsStatusNode}.
   *
   * @param status the ufs status
   */
  protected UfsStatusNode(UfsStatus status) {
    mUfsStatus = status;
  }

  public boolean isDirectory() {
    return mUfsStatus.isDirectory();
  }

  public boolean isFile() {
    return mUfsStatus.isFile();
  }

  public UfsStatus getStatus() {
    return mUfsStatus;
  }

  /**
   *
   * @param name
   * @param child
   */
  public synchronized void addChild(String name, UfsStatusNode child) {
    if (child.isDirectory()) {
      mChildDirs.put(name, child);
    } else {
      mChildFiles.put(name, child);
    }
  }

  /**
   * Get a child directory.
   *
   * @param name the name of the subdirectory to retrieve. This should be simple.
   * @return
   */
  @Nullable
  public UfsStatusNode getChildDir(String name) {
    if (isFile()) {
      return null;
    }
    return mChildDirs.get(name);
  }

}
