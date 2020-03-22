package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.exception.AccessControlException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.file.meta.UfsSyncUtils;
import alluxio.master.metastore.InodeStore;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UfsStatusCache;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;
import alluxio.util.interfaces.Scoped;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

public class InodeSyncStream {
  private static final Logger LOG = LoggerFactory.getLogger(InodeSyncStream.class);

  private final UfsSyncPathCache mUfsSyncPathCache;
  private final UfsStatusCache mStatusCache;
  private final InodeTree mInodeTree;
  private final FileSystemMasterCommonPOptions mOptions;
  private final boolean mIsGetFileInfo;
  private final DescendantType mDescendantType;
  private final RpcContext mRpcContext;
  private final InodeStore mInodeStore;
  private final MountTable mMountTable;
  private final InodeLockManager mInodeLockManager;
  private final DefaultFileSystemMaster mFsMaster;

  private final ConcurrentLinkedQueue<AlluxioURI> mInodes;

  public InodeSyncStream(AlluxioURI rootPath, DefaultFileSystemMaster fsMaster,
      InodeTree inodeTree, InodeStore inodeStore,
      InodeLockManager inodeLockManager,
      MountTable mountTable, RpcContext rpcContext, DescendantType descendantType,
      UfsSyncPathCache ufsSyncPathCache,  FileSystemMasterCommonPOptions options,
      boolean isGetFileInfo) {
    mDescendantType = descendantType;
    mFsMaster = fsMaster;
    mIsGetFileInfo = isGetFileInfo;
    mInodes = new ConcurrentLinkedQueue<>();
    mInodeLockManager = inodeLockManager;
    mInodeStore = inodeStore;
    mInodeTree = inodeTree;
    mMountTable = mountTable;
    mOptions = options;
    mRpcContext = rpcContext;
    mStatusCache = new UfsStatusCache(rootPath);
    mUfsSyncPathCache = ufsSyncPathCache;
    mInodes.add(rootPath);
  }

  private void sync() {
  }

  private void syncPathMetadata(AlluxioURI path) {
    LockingScheme scheme = new LockingScheme(path, InodeTree.LockPattern.WRITE_EDGE, mOptions,
        mUfsSyncPathCache, mIsGetFileInfo);
    try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, scheme)) {
      // The high-level process for the syncing is:
      // 1. Find all Alluxio paths which are not consistent with the corresponding UFS path.
      //    This means the UFS path does not exist, or is different from the Alluxio metadata.
      // 2. If only the metadata changed for a file or a directory, update the inode with
      //    new metadata from the UFS.
      // 3. Delete any Alluxio path whose content is not consistent with UFS, or not in UFS. After
      //    this step, all the paths in Alluxio are consistent with UFS, and there may be additional
      //    UFS paths to load.
      // 4. Load metadata from UFS.
      syncInodeMetadata(inodePath);

    } catch (FileDoesNotExistException | InvalidPathException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (AccessControlException e) {
      e.printStackTrace();
    }
  }

  /**
   * This function will return a sync result denoting whether this
   */
  private SyncResult syncInodeMetadata(LockedInodePath inodePath)
      throws FileDoesNotExistException, IOException, InvalidPathException, AccessControlException {
    Preconditions.checkState(inodePath.getLockPattern() == LockPattern.WRITE_EDGE);

    if (Thread.currentThread().isInterrupted()) {
      LOG.warn("Thread syncing {} was interrupted before completion", inodePath.getUri());
      return SyncResult.defaults();
    }
    // Set to true if the given inode was deleted.
    boolean deletedInode = false;
    // Set of paths to sync
    Set<String> pathsToLoad = new HashSet<>();
    LOG.debug("Syncing inode metadata {}", inodePath.getUri());

    // The requested path already exists in Alluxio.
    Inode inode = inodePath.getInode();

    if (inode instanceof InodeFile && !inode.asFile().isCompleted()) {
      // Do not sync an incomplete file, since the UFS file is expected to not exist.
      return SyncResult.defaults();
    }

    Optional<Scoped> persistingLock = mInodeLockManager.tryAcquirePersistingLock(inode.getId());
    if (!persistingLock.isPresent()) {
      // Do not sync a file in the process of being persisted, since the UFS file is being
      // written.
      return SyncResult.defaults();
    }
    persistingLock.get().close();

    MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
    AlluxioURI ufsUri = resolution.getUri();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      String ufsFingerprint;
      Fingerprint ufsFpParsed;
      UfsStatus cachedStatus = mStatusCache.get(inodePath.getUri());
      if (cachedStatus == null) {
        // TODO(david): change the interface so that getFingerprint returns a parsed fingerprint
        ufsFingerprint = ufs.getFingerprint(ufsUri.toString());
        ufsFpParsed = Fingerprint.parse(ufsFingerprint);
      } else {
        Pair<AccessControlList, DefaultAccessControlList> aclPair
            = ufs.getAclPair(ufsUri.toString());

        if (aclPair == null || aclPair.getFirst() == null || !aclPair.getFirst().hasExtended()) {
          ufsFpParsed = Fingerprint.create(ufs.getUnderFSType(), cachedStatus);
        } else {
          ufsFpParsed = Fingerprint.create(ufs.getUnderFSType(), cachedStatus,
              aclPair.getFirst());
        }
        ufsFingerprint = ufsFpParsed.serialize();
      }

      boolean containsMountPoint = mMountTable.containsMountPoint(inodePath.getUri(), true);

      UfsSyncUtils.SyncPlan syncPlan =
          UfsSyncUtils.computeSyncPlan(inode, ufsFpParsed, containsMountPoint);

      if (syncPlan.toUpdateMetaData()) {
        // UpdateMetadata is used when a file or a directory only had metadata change.
        // It works by calling SetAttributeInternal on the inodePath.
        if (ufsFpParsed != null && ufsFpParsed.isValid()) {
          short mode = Short.parseShort(ufsFpParsed.getTag(Fingerprint.Tag.MODE));
          long opTimeMs = System.currentTimeMillis();
          mFsMaster.setAttributeSingleFile(mRpcContext, inodePath, false, opTimeMs,
              SetAttributeContext
              .mergeFrom(SetAttributePOptions.newBuilder().setOwner(ufsFpParsed.getTag(
                  Fingerprint.Tag.OWNER))
                  .setGroup(ufsFpParsed.getTag(Fingerprint.Tag.GROUP)).setMode(new Mode(mode).toProto()))
              .setUfsFingerprint(ufsFingerprint));
        }
      }
      if (syncPlan.toDelete()) {
        try {
          // The options for deleting.
          DeleteContext syncDeleteContext = DeleteContext.mergeFrom(DeletePOptions.newBuilder()
              .setRecursive(true)
              .setAlluxioOnly(true)
              .setUnchecked(true));
          mFsMaster.deleteInternal(mRpcContext, inodePath, syncDeleteContext);

          deletedInode = true;
        } catch (DirectoryNotEmptyException | IOException e) {
          // Should not happen, since it is an unchecked delete.
          LOG.error("Unexpected error for unchecked delete.", e);
        }
      }
      if (syncPlan.toLoadMetadata()) {
        AlluxioURI mountUri = new AlluxioURI(mMountTable.getMountPoint(inodePath.getUri()));
        pathsToLoad.add(mountUri.getPath());
      }
      if (syncPlan.toSyncChildren() && inode.isDirectory()
          && mDescendantType != DescendantType.NONE) {
        // maps children name to inode
        Map<String, Inode> inodeChildren = new HashMap<>();
        for (Inode child : mInodeStore.getChildren(inode.asDirectory())) {
          inodeChildren.put(child.getName(), child);
        }

        UfsStatus[] listStatus = ufs.listStatus(ufsUri.toString(), ListOptions.defaults());
        // Iterate over UFS listings and process UFS children.
        if (listStatus != null) {
          for (UfsStatus ufsChildStatus : listStatus) {
            if (!inodeChildren.containsKey(ufsChildStatus.getName()) && !PathUtils
                .isTemporaryFileName(ufsChildStatus.getName())) {
              // Ufs child exists, but Alluxio child does not. Must load metadata.
              AlluxioURI mountUri = new AlluxioURI(mMountTable.getMountPoint(inodePath.getUri()));
              pathsToLoad.add(mountUri.getPath());
              break;
            }
          }
        }

        // Iterate over Alluxio children and process persisted children.
        Stream<String> childStream = inodeChildren.keySet().stream();
        childStream.forEach(childPath -> {
          // If we're performing a recusive sync, add each child of our current Inode to the queue
          AlluxioURI child = inodePath.getUri().joinUnsafe(childPath);
          mInodes.add(child);
        });
      }
    }
    return new SyncResult(deletedInode, pathsToLoad);
  }

  /**
   * This class represents the result for a sync. The following are returned:
   * - deleted: if true, the inode was already deleted as part of the syncing process
   * - pathsToLoad: a set of paths that need to be loaded from UFS.
   */
  private static class SyncResult {
    private boolean mDeletedInode;
    private Set<String> mPathsToLoad;

    static SyncResult defaults() {
      return new SyncResult(false, new HashSet<>());
    }

    SyncResult(boolean deletedInode, Set<String> pathsToLoad) {
      mDeletedInode = deletedInode;
      mPathsToLoad = new HashSet<>(pathsToLoad);
    }

    boolean getDeletedInode() {
      return mDeletedInode;
    }

    Set<String> getPathsToLoad() {
      return mPathsToLoad;
    }
}
}
