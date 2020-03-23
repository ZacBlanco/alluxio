package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.collections.Pair;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.LoadMetadataContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.file.meta.UfsSyncUtils;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UfsStatusCache2;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;
import alluxio.util.interfaces.Scoped;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The purpose of this class is to sync Inode metadata in a stream-like fashion.
 *
 */
public class InodeSyncStream {
  private static final Logger LOG = LoggerFactory.getLogger(InodeSyncStream.class);

  private final UfsSyncPathCache mUfsSyncPathCache;
  private final UfsStatusCache2 mStatusCache;
  private final InodeTree mInodeTree;
  private final FileSystemMasterCommonPOptions mOptions;
  private final boolean mIsGetFileInfo;
  private final DescendantType mDescendantType;
  private final RpcContext mRpcContext;
  private final ReadOnlyInodeStore mInodeStore;
  private final MountTable mMountTable;
  private final InodeLockManager mInodeLockManager;
  private final DefaultFileSystemMaster mFsMaster;

  private final ConcurrentLinkedQueue<AlluxioURI> mSyncMetadataQ;

  /**
   * Create a new instance of {@link InodeSyncStream}.
   *
   * @param rootPath a
   * @param fsMaster a
   * @param inodeTree a
   * @param inodeStore a
   * @param inodeLockManager a
   * @param mountTable a
   * @param rpcContext a
   * @param descendantType a
   * @param ufsSyncPathCache a
   * @param options a
   * @param isGetFileInfo a
   */
  public InodeSyncStream(AlluxioURI rootPath, DefaultFileSystemMaster fsMaster,
      InodeTree inodeTree, ReadOnlyInodeStore inodeStore, InodeLockManager inodeLockManager,
      MountTable mountTable, RpcContext rpcContext, DescendantType descendantType,
      UfsSyncPathCache ufsSyncPathCache,  FileSystemMasterCommonPOptions options,
      boolean isGetFileInfo) {
    mDescendantType = descendantType;
    mFsMaster = fsMaster;
    mIsGetFileInfo = isGetFileInfo;
    mSyncMetadataQ = new ConcurrentLinkedQueue<>();
    mInodeLockManager = inodeLockManager;
    mInodeStore = inodeStore;
    mInodeTree = inodeTree;
    mMountTable = mountTable;
    mOptions = options;
    mRpcContext = rpcContext;
    mStatusCache = new UfsStatusCache2();
    mUfsSyncPathCache = ufsSyncPathCache;
    mSyncMetadataQ.add(rootPath);
  }

  /**
   * Sync the metadata according the the root path the stream was created with.
   *
   * @return true if at least one path was synced
   */
  public boolean sync() {
    // The high-level process for the syncing is:
    // 1. Find all Alluxio paths which are not consistent with the corresponding UFS path.
    //    This means the UFS path does not exist, or is different from the Alluxio metadata.
    // 2. If only the metadata changed for a file or a directory, update the inode with
    //    new metadata from the UFS.
    // 3. Delete any Alluxio path whose content is not consistent with UFS, or not in UFS. After
    //    this step, all the paths in Alluxio are consistent with UFS, and there may be additional
    //    UFS paths to load.
    // 4. Load metadata from UFS.
    int syncPathCount = 0;
    while (!mSyncMetadataQ.isEmpty()) {
      AlluxioURI path = mSyncMetadataQ.poll();
      LockingScheme scheme = new LockingScheme(path, InodeTree.LockPattern.WRITE_EDGE, mOptions,
          mUfsSyncPathCache, mIsGetFileInfo);
      if (!scheme.shouldSync()) {
        continue;
      }
      try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, scheme)) {
        syncInodeMetadata(inodePath);
        syncPathCount++;
      } catch (AccessControlException | BlockInfoException | FileAlreadyCompletedException
          | FileDoesNotExistException | InvalidFileSizeException | InvalidPathException
          | IOException e) {
        LOG.warn("FAILED TO SYNC METADATA: {}", e.getMessage(), e);
      } finally {
        // regardless of the outcome, remove the UfsStatus for this path from the cache
        mStatusCache.remove(path);
      }
    }
    return syncPathCount > 0;
  }

  /**
   * Sync inode metadata with the UFS state.
   */
  private SyncResult syncInodeMetadata(LockedInodePath inodePath)
      throws AccessControlException, BlockInfoException, FileAlreadyCompletedException,
      FileDoesNotExistException, InvalidFileSizeException, InvalidPathException, IOException,
      InvalidPathException {
    Preconditions.checkState(inodePath.getLockPattern() == LockPattern.WRITE_EDGE);

    if (Thread.currentThread().isInterrupted()) {
      LOG.warn("Thread syncing {} was interrupted before completion", inodePath.getUri());
      return SyncResult.defaults();
    }
    // Set to true if the given inode was deleted.
    boolean deletedInode = false;
    // Set of paths to sync
    boolean loadMetadata = false;
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
      UfsStatus cachedStatus = mStatusCache.getStatus(inodePath.getUri());
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
                  .setGroup(ufsFpParsed.getTag(Fingerprint.Tag.GROUP))
                  .setMode(new Mode(mode).toProto()))
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
        // Get the parent path of the current inode,
//        AlluxioURI parent = inodePath.getUri().getParent();
//        if (parent == null) {
//          parent = new AlluxioURI("/");
//        }
//        pathsToLoad.add(parent);
        loadMetadata = true;
      }

      boolean syncChildren = syncPlan.toSyncChildren()
          && inode.isDirectory()
          && mDescendantType != DescendantType.NONE;
      if (syncChildren) {
        // maps children name to inode
        Map<String, Inode> inodeChildren = new HashMap<>();
        for (Inode child : mInodeStore.getChildren(inode.asDirectory())) {
          inodeChildren.put(child.getName(), child);
        }

        UfsStatus[] listStatus = ufs.listStatus(ufsUri.toString(), ListOptions.defaults());
        // Iterate over UFS listings and process UFS children.
        if (listStatus != null) {
          // First, add the statuses to the cache
          mStatusCache.addChildren(inodePath.getUri(), Arrays.asList(listStatus));

          for (UfsStatus ufsChildStatus : listStatus) {
            if (!inodeChildren.containsKey(ufsChildStatus.getName()) && !PathUtils
                .isTemporaryFileName(ufsChildStatus.getName())) {
              // Ufs child exists, but Alluxio child does not. Must load metadata.
              loadMetadata = true;
              break;
            }
          }
        }
      }

      // load metadata if necessary.
      if (loadMetadata) {
        LoadMetadataContext ctx = LoadMetadataContext.defaults()
            .setUfsStatus(mStatusCache.getStatus(inodePath.getUri()));
        loadMetadata(inodePath, ctx);
      }
      mUfsSyncPathCache.notifySyncedPath(inodePath.getUri().getPath(), DescendantType.ONE);

      if (syncChildren) {
        // Iterate over Alluxio children and process persisted children.
        mInodeStore.getChildren(inode.asDirectory()).forEach(childInode -> {
          // If we're performing a recusive sync, add each child of our current Inode to the queue
          AlluxioURI child = inodePath.getUri().joinUnsafe(childInode.getName());
          mSyncMetadataQ.add(child);
        });
      }
    }

    return new SyncResult(deletedInode, new HashSet<>());
  }

  private void loadMetadata(LockedInodePath inodePath, LoadMetadataContext context)
      throws AccessControlException, BlockInfoException, FileAlreadyCompletedException,
      FileDoesNotExistException, InvalidFileSizeException, InvalidPathException, IOException {
    AlluxioURI path = inodePath.getUri();
    MountTable.Resolution resolution = mMountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      if (context.getUfsStatus() == null && !ufs.exists(ufsUri.toString())) {
        // uri does not exist in ufs
        InodeDirectory inode = inodePath.getInode().asDirectory();
        mInodeTree.setDirectChildrenLoaded(mRpcContext, inode);
        return;
      }
      boolean isFile;
      if (context.getUfsStatus() != null) {
        isFile = context.getUfsStatus().isFile();
      } else {
        isFile = ufs.isFile(ufsUri.toString());
      }
      if (isFile) {
        loadFileMetadataInternal(mRpcContext, inodePath, resolution, context);
      } else {
        loadDirectoryMetadata(mRpcContext, inodePath, context);

        // now load all children if required
        LoadDescendantPType type = context.getOptions().getLoadDescendantType();
        if (type != LoadDescendantPType.NONE) {
          Collection<UfsStatus> children = mStatusCache.getChildren(inodePath.getUri());
          if (children == null) {
            UfsStatus[] childStatuses = ufs.listStatus(ufsUri.toString());
            if (childStatuses == null) {
              return;
            }
            children = Arrays.asList(childStatuses);
            mStatusCache.addChildren(inodePath.getUri(), children);
          }
          children = mStatusCache.getChildren(inodePath.getUri());
          for (UfsStatus childStatus : children) {
            if (PathUtils.isTemporaryFileName(childStatus.getName())) {
              continue;
            }
            AlluxioURI childURI = new AlluxioURI(PathUtils.concatPath(inodePath.getUri(),
                childStatus.getName()));
            if (mInodeTree.inodePathExists(childURI) && (childStatus.isFile()
                || context.getOptions().getLoadDescendantType() != LoadDescendantPType.ALL)) {
              // stop traversing if this is an existing file, or an existing directory without
              // loading all descendants.
              continue;
            }
            LoadMetadataContext loadMetadataContext = LoadMetadataContext.mergeFrom(
                LoadMetadataPOptions.newBuilder().setLoadDescendantType(LoadDescendantPType.NONE)
                    .setCreateAncestors(false)).setUfsStatus(childStatus);
            try (LockedInodePath descendant = inodePath
                .lockDescendant(inodePath.getUri().joinUnsafe(childStatus.getName()),
                    LockPattern.READ)) {
              loadMetadata(descendant, loadMetadataContext);
            } catch (FileNotFoundException e) {
              LOG.debug("Failed to loadMetadata because file is not in ufs:"
                      + " inodePath={}, options={}.",
                  childURI, loadMetadataContext, e);
            }
          }
          mInodeTree.setDirectChildrenLoaded(mRpcContext, inodePath.getInode().asDirectory());
        }
      }
    } catch (IOException e) {
      LOG.debug("Failed to loadMetadata: inodePath={}, context={}.", inodePath.getUri(), context,
          e);
      throw e;
    }
  }

  /**
   * Loads metadata for the file identified by the given path from UFS into Alluxio.
   *
   * This method doesn't require any specific type of locking on inodePath. If the path needs to be
   * loaded, we will acquire a write-edge lock.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path for which metadata should be loaded
   * @param resolution the UFS resolution of path
   * @param context the load metadata context
   */
  private void loadFileMetadataInternal(RpcContext rpcContext, LockedInodePath inodePath,
      MountTable.Resolution resolution, LoadMetadataContext context)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      FileAlreadyCompletedException, InvalidFileSizeException, IOException {
    if (inodePath.fullPathExists()) {
      return;
    }
    AlluxioURI ufsUri = resolution.getUri();
    long ufsBlockSizeByte;
    long ufsLength;
    AccessControlList acl = null;
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();

      if (context.getUfsStatus() == null) {
        context.setUfsStatus(ufs.getExistingFileStatus(ufsUri.toString()));
      }
      ufsLength = ((UfsFileStatus) context.getUfsStatus()).getContentLength();
      long blockSize = ((UfsFileStatus) context.getUfsStatus()).getBlockSize();
      ufsBlockSizeByte = blockSize != UfsFileStatus.UNKNOWN_BLOCK_SIZE
          ? blockSize : ufs.getBlockSizeByte(ufsUri.toString());

      if (mFsMaster.isAclEnabled()) {
        Pair<AccessControlList, DefaultAccessControlList> aclPair
            = ufs.getAclPair(ufsUri.toString());
        if (aclPair != null) {
          acl = aclPair.getFirst();
          // DefaultACL should be null, because it is a file
          if (aclPair.getSecond() != null) {
            LOG.warn("File {} has default ACL in the UFS", inodePath.getUri());
          }
        }
      }
    }

    // Metadata loaded from UFS has no TTL set.
    CreateFileContext createFileContext = CreateFileContext.defaults();
    createFileContext.getOptions().setBlockSizeBytes(ufsBlockSizeByte);
    createFileContext.getOptions().setRecursive(context.getOptions().getCreateAncestors());
    createFileContext.getOptions()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(context.getOptions().getCommonOptions().getTtl())
            .setTtlAction(context.getOptions().getCommonOptions().getTtlAction()));
    createFileContext.setWriteType(WriteType.THROUGH); // set as through since already in UFS
    createFileContext.setMetadataLoad(true);
    createFileContext.setOwner(context.getUfsStatus().getOwner());
    createFileContext.setGroup(context.getUfsStatus().getGroup());
    createFileContext.setXAttr(context.getUfsStatus().getXAttr());
    short ufsMode = context.getUfsStatus().getMode();
    Mode mode = new Mode(ufsMode);
    Long ufsLastModified = context.getUfsStatus().getLastModifiedTime();
    if (resolution.getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createFileContext.getOptions().setMode(mode.toProto());
    if (acl != null) {
      createFileContext.setAcl(acl.getEntries());
    }
    if (ufsLastModified != null) {
      createFileContext.setOperationTimeMs(ufsLastModified);
    }

    try (LockedInodePath writeLockedPath = inodePath.lockFinalEdgeWrite()) {
      mFsMaster.createFileInternal(rpcContext, writeLockedPath, createFileContext);
      CompleteFileContext completeContext =
          CompleteFileContext.mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(ufsLength))
              .setUfsStatus(context.getUfsStatus());
      if (ufsLastModified != null) {
        completeContext.setOperationTimeMs(ufsLastModified);
      }
      mFsMaster.completeFileInternal(rpcContext, writeLockedPath, completeContext);
    } catch (FileAlreadyExistsException e) {
      // This may occur if a thread created or loaded the file before we got the write lock.
      // The file already exists, so nothing needs to be loaded.
      LOG.debug("Failed to load file metadata: {}", e.toString());
    }
    // Re-traverse the path to pick up any newly created inodes.
    inodePath.traverse();
  }

  /**
   * Loads metadata for the directory identified by the given path from UFS into Alluxio. This does
   * not actually require looking at the UFS path.
   * It is a no-op if the directory exists.
   *
   * This method doesn't require any specific type of locking on inodePath. If the path needs to be
   * loaded, we will acquire a write-edge lock if necessary.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path for which metadata should be loaded
   * @param context the load metadata context
   */
  private void loadDirectoryMetadata(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException, IOException {
    if (inodePath.fullPathExists()) {
      return;
    }
    CreateDirectoryContext createDirectoryContext = CreateDirectoryContext.defaults();
    createDirectoryContext.getOptions()
        .setRecursive(context.getOptions().getCreateAncestors()).setAllowExists(false)
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(context.getOptions().getCommonOptions().getTtl())
            .setTtlAction(context.getOptions().getCommonOptions().getTtlAction()));
    createDirectoryContext.setMountPoint(mMountTable.isMountPoint(inodePath.getUri()));
    createDirectoryContext.setMetadataLoad(true);
    createDirectoryContext.setWriteType(WriteType.THROUGH);
    MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());

    AlluxioURI ufsUri = resolution.getUri();
    AccessControlList acl = null;
    DefaultAccessControlList defaultAcl = null;
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      if (context.getUfsStatus() == null) {
        context.setUfsStatus(ufs.getExistingDirectoryStatus(ufsUri.toString()));
      }
      Pair<AccessControlList, DefaultAccessControlList> aclPair =
          ufs.getAclPair(ufsUri.toString());
      if (aclPair != null) {
        acl = aclPair.getFirst();
        defaultAcl = aclPair.getSecond();
      }
    }
    String ufsOwner = context.getUfsStatus().getOwner();
    String ufsGroup = context.getUfsStatus().getGroup();
    short ufsMode = context.getUfsStatus().getMode();
    Long lastModifiedTime = context.getUfsStatus().getLastModifiedTime();
    Mode mode = new Mode(ufsMode);
    if (resolution.getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createDirectoryContext.getOptions().setMode(mode.toProto());
    createDirectoryContext.setOwner(ufsOwner).setGroup(ufsGroup)
        .setUfsStatus(context.getUfsStatus());
    createDirectoryContext.setXAttr(context.getUfsStatus().getXAttr());
    if (acl != null) {
      createDirectoryContext.setAcl(acl.getEntries());
    }

    if (defaultAcl != null) {
      createDirectoryContext.setDefaultAcl(defaultAcl.getEntries());
    }
    if (lastModifiedTime != null) {
      createDirectoryContext.setOperationTimeMs(lastModifiedTime);
    }

    LOG.warn("lockFinalWriteEdge: {}", inodePath);
    try (LockedInodePath writeLockedPath = inodePath.lockFinalEdgeWrite()) {
      mFsMaster.createDirectoryInternal(rpcContext, writeLockedPath, createDirectoryContext);
    } catch (FileAlreadyExistsException e) {
      // This may occur if a thread created or loaded the directory before we got the write lock.
      // The directory already exists, so nothing needs to be loaded.
    }
    // Re-traverse the path to pick up any newly created inodes.
    inodePath.traverse();
  }

  /**
   * This class represents the result for a sync. The following are returned:
   * - deleted: if true, the inode was already deleted as part of the syncing process
   * - pathsToLoad: a set of paths that need to be loaded from UFS.
   */
  private static class SyncResult {
    private boolean mDeletedInode;
    private Set<AlluxioURI> mPathsToLoad;

    static SyncResult defaults() {
      return new SyncResult(false, new HashSet<>());
    }

    SyncResult(boolean deletedInode, Set<AlluxioURI> pathsToLoad) {
      mDeletedInode = deletedInode;
      mPathsToLoad = new HashSet<>(pathsToLoad);
    }

    boolean getDeletedInode() {
      return mDeletedInode;
    }

    Set<AlluxioURI> getPathsToLoad() {
      return mPathsToLoad;
    }
  }
}
