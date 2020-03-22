package alluxio.underfs;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The representation of an inode from a UfsStatus
 */
@ThreadSafe
public class UfsStatusNode {
  private final UfsStatus mUfsStatus;
  private final Map<String, UfsStatusNode> mChildDirs = new ConcurrentHashMap<>();
  private final Map<String, UfsStatusNode> mChildFiles = new ConcurrentHashMap<>();

  /**
   * Create a new instance of {@link UfsStatusNode}.
   *
   * @param status the ufs status. If null, this represent a node which
   */
  protected UfsStatusNode(@Nullable UfsStatus status, HashMap<String, UfsStatus> children) {
    mUfsStatus = status;
    if (children != null) {
      for (Map.Entry<String, UfsStatus> entry : children.entrySet()) {
        if (entry.getValue().isFile()) {
          mChildFiles.put(entry.getKey(), new UfsStatusNode(entry.getValue()));
        } else if (entry.getValue().isDirectory()) {
          mChildDirs.put(entry.getKey(), new UfsStatusNode(entry.getValue()));
        }
      }
    }
  }

  /**
   * Create a new instance of {@link UfsStatusNode}.
   *
   * @param status the ufs status. If null, this represent a node which
   */
  protected UfsStatusNode(@Nullable UfsStatus status) {
    this(status, null);
  }

  /**
   * Create a new instance of {@link UfsStatusNode}.
   */
  protected UfsStatusNode() {
    this(null);
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

  /**
   * Get a child directory.
   *
   * @param name the name of the file to retrieve.
   * @return
   */
  @Nullable
  public UfsStatusNode getChildFile(String name) {
    if (isFile()) {
      return null;
    }
    return mChildFiles.get(name);
  }

  public Collection<UfsStatus> getChildren() {
    // Avoid resizing the list since we know the size already
    List<UfsStatus> children = new ArrayList<>(mChildDirs.size() + mChildFiles.size());
    children.addAll(mChildFiles.values().stream()
        .map(UfsStatusNode::getStatus).collect(Collectors.toList()));
    children.addAll(mChildDirs.values().stream()
        .map(UfsStatusNode::getStatus).collect(Collectors.toList()));
    return children;
  }

}
