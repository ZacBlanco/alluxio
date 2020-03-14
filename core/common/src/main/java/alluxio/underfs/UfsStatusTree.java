package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * UfsStatusTree represents a number of UfsStatus object in tree structures.
 *
 * This allows for fast querying and easy traversal of listStatus calls made
 * to a UFS.
 */
@ThreadSafe
public class UfsStatusTree {

  private final UfsStatusNode mRoot;
  private final ConcurrentHashMap<Integer, ConcurrentHashSet<UfsStatusNode>> mTreeLevels;

  /**
   * Disallow instantiation through constructor. Use {@link Builder} instead.
   */
  private UfsStatusTree() {
    throw new RuntimeException("Attempted to create UfsStatus tree with private constructor. This"
        + " method is not implemented.");
  }

  public UfsStatusTree(UfsStatusNode root,
      ConcurrentHashMap<Integer, ConcurrentHashSet<UfsStatusNode>> treeLevels) {
    mRoot = root;
    mTreeLevels = treeLevels;
  }

  public Iterator<Stream<UfsStatus>> levelOrderTraversal() {
    List<Integer> levels = mTreeLevels
        .keySet().stream().filter(k -> k != 0).sorted().collect(Collectors.toList());
    Iterator<Integer> levelIterator = levels.iterator();
    return new Iterator<Stream<UfsStatus>>() {
      @Override public boolean hasNext() {
        return levelIterator.hasNext();
      }

      @Override public Stream<UfsStatus> next() {
        ConcurrentHashSet<UfsStatusNode> statusNodes = mTreeLevels.get(levelIterator.next());
        return statusNodes.stream().map(UfsStatusNode::getStatus);
      }
    };
  }

  @Nullable
  public UfsStatus get(String path) {
    StringTokenizer tok = new StringTokenizer(path, AlluxioURI.SEPARATOR);
    UfsStatusNode currNode = mRoot;
    UfsStatusNode prevNode = null;
    String pathComponent = null;
    while (tok.hasMoreTokens()) {
      pathComponent = tok.nextToken();
      // We're at the root of the tree, so the current token is what must be added into the tree
      prevNode = currNode;
      if (prevNode != null) {
        currNode = mRoot.getChildDir(pathComponent);
      }
    }
    if (currNode == null) {
      return null;
    }
    return currNode.getStatus();
  }

  /**
   * The class used to generate the immutable {@link UfsStatusTree}
   */
  @ThreadSafe
  public static class Builder {
    private final ConcurrentHashMap<Integer, ConcurrentHashSet<UfsStatusNode>> mTreeLevels;
    private final UfsStatusNode mRoot;
    public Builder(String root) {
      UfsStatus stat = new UfsDirectoryStatus(root, "", "", (short) 0, -1L);
      mRoot = new UfsStatusNode(stat);
      mTreeLevels = new ConcurrentHashMap<>();
      mTreeLevels.computeIfAbsent(0, (l) -> new ConcurrentHashSet<>()).add(mRoot);
    }

    /**
     * Add a UFS status to the tree.
     *
     * The statuses' names are expected to represent the relative path to the directory, not just
     * the name of the dir or folder.
     *
     * @param status the Ufs Status to add to the tree.
     */
    public void addUfsStatus(UfsStatus status) {
      StringTokenizer tok = new StringTokenizer(status.getName(), AlluxioURI.SEPARATOR);
      int levels = 0;
      UfsStatusNode currNode = mRoot;
      UfsStatusNode prevNode = null;
      String pathComponent = null;
      while (tok.hasMoreTokens()) {
        if (currNode == null) {
          // Tried to add a Ufs status to a node which doesn't exist yet
          throw new IllegalArgumentException(
              String.format("Tried to create a node (%s) whose parent does not yet exist in the"
                  + " tree.", status.getName()));
        }
        pathComponent = tok.nextToken();
        // We're at the root of the tree, so the current token is what must be added into the tree
        prevNode = currNode;
        currNode = mRoot.getChildDir(pathComponent);
        levels++;
      }

      if (prevNode == null) {
        // There were no tokens in the string. Bad UFS status name.
        throw new IllegalArgumentException(String.format("Failed to tokenize %s on /",
            status.getName()));
      }

      // If it still non-null, this status already exists.
      if (currNode != null) {
        throw new IllegalArgumentException(String.format("UfsStatus already in tree: %s", status));
      }
      // prevNode contains the node we need to add the status to
      UfsStatusNode statusNode = new UfsStatusNode(status);
      mTreeLevels.computeIfAbsent(levels, (l) -> new ConcurrentHashSet<>()).add(statusNode);
      prevNode.addChild(pathComponent, statusNode);
    }

    public UfsStatusTree build() {
      return new UfsStatusTree(mRoot, mTreeLevels);
    }
  }
}
