/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

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
import alluxio.grpc.GrpcUtils;
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
import alluxio.util.interfaces.Scoped;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The purpose of this class is to sync Inode metadata in a stream-like fashion.
 *
 */
public class InodeSyncStream {
  private static final Logger LOG = LoggerFactory.getLogger(InodeSyncStream.class);

  private final LockedInodePath mRootPath;
  private final UfsSyncPathCache mUfsSyncPathCache;
  private final UfsStatusCache2 mStatusCache;
  private final InodeTree mInodeTree;
  private final DescendantType mDescendantType;
  private final RpcContext mRpcContext;
  private final ReadOnlyInodeStore mInodeStore;
  private final MountTable mMountTable;
  private final InodeLockManager mInodeLockManager;
  private final DefaultFileSystemMaster mFsMaster;
  private final boolean mShouldSync;
  private final FileSystemMasterCommonPOptions mSyncOptions;
  private final boolean mIsGetFileInfo;
  private final boolean mLoadOnly;

  private final ConcurrentLinkedQueue<AlluxioURI> mSyncMetadataQ;

  /**
   * Create a new instance of {@link InodeSyncStream}.
   *
   * The root path should be already locked with {@link LockPattern#WRITE_EDGE} unless the user is
   * only planning on loading metadata. The desired pattern should always be
   * {@link LockPattern#READ}.
   *
   * It is an error to initiate sync without a WRITE_EDGE lock when loadOnly is {@code false}.
   * If loadOnly is set to {@code true}
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
   * @param forceSync a
   * @param loadOnly a
   */
  public InodeSyncStream(LockedInodePath rootPath, DefaultFileSystemMaster fsMaster,
      InodeTree inodeTree, ReadOnlyInodeStore inodeStore, InodeLockManager inodeLockManager,
      MountTable mountTable, RpcContext rpcContext, DescendantType descendantType,
      UfsSyncPathCache ufsSyncPathCache, FileSystemMasterCommonPOptions options,
      boolean isGetFileInfo, boolean forceSync, boolean loadOnly) {
    mDescendantType = descendantType;
    mFsMaster = fsMaster;
    mSyncMetadataQ = new ConcurrentLinkedQueue<>();
    mInodeLockManager = inodeLockManager;
    mInodeStore = inodeStore;
    mInodeTree = inodeTree;
    mMountTable = mountTable;
    mRpcContext = rpcContext;
    mStatusCache = new UfsStatusCache2(fsMaster.mSyncPrefetchExecutor);
    mUfsSyncPathCache = ufsSyncPathCache;
    mShouldSync = forceSync;
    mRootPath = rootPath;
    mSyncOptions = options;
    mIsGetFileInfo = isGetFileInfo;
    mLoadOnly = loadOnly;
  }

  /**
   * Sync the metadata according the the root path the stream was created with.
   *
   * @return true if at least one path was synced
   */
  public boolean sync() {
    // The high-level process for the syncing is:
    // 1. Given an Alluxio path, determine if it is not consistent with the corresponding UFS path.
    //     this means the UFS path does not exist, or has metadata which differs from Alluxio
    // 2. If only the metadata changed, update the inode with the new metadata
    // 3. If the path does not exist in the UFS, delete the inode in Alluxio
    // 4. If not deleted, load metadata from the UFS
    // 5. If a recursive sync, add children inodes to sync queue
    int syncPathCount = 0;
    int stopNum = -1; // stop syncing when we've processed this many paths. -1 for infinite

    try {
      syncInodeMetadata(mRootPath);
      syncPathCount++;
      if (mDescendantType == DescendantType.ONE) {
        // If descendantType is ONE, then we shouldn't process any more paths except for those
        // currently in the queue
        stopNum = mSyncMetadataQ.size();
      }

      // process the sync result for the original path
      try {
        mRootPath.traverse();
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } catch (AccessControlException | BlockInfoException | FileAlreadyCompletedException
        | FileDoesNotExistException | InvalidFileSizeException | InvalidPathException
        | IOException e) {
      LOG.warn("FAILED TO SYNC METADATA: {}", e.getMessage(), e);
    } finally {
      // regardless of the outcome, remove the UfsStatus for this path from the cache
      mStatusCache.remove(mRootPath.getUri());
      // downgrade so that if operations are parallelized, the lock on the root doesn't restrict
      // concurrent operations
      mRootPath.downgradeToPattern(LockPattern.READ);
    }

    // Process any children after the root.
    while (!mSyncMetadataQ.isEmpty()) {
      AlluxioURI path = mSyncMetadataQ.poll();
      LockingScheme scheme;
      if (mShouldSync) {
        scheme = new LockingScheme(path, LockPattern.READ, true);
      } else {
        scheme = new LockingScheme(path, LockPattern.READ, mSyncOptions,
            mUfsSyncPathCache, mIsGetFileInfo);
      }

      if (!scheme.shouldSync() && !mShouldSync) {
        continue;
      }
      try (LockedInodePath inodePath = mInodeTree.lockInodePath(scheme)) {
        if (Thread.currentThread().isInterrupted()) {
          LOG.warn("Thread syncing {} was interrupted before completion", inodePath.getUri());
          break;
        }
        syncInodeMetadata(inodePath);
        syncPathCount++;
        if (stopNum != -1 && syncPathCount > stopNum) {
          break;
        }
      } catch (AccessControlException | BlockInfoException | FileAlreadyCompletedException
          | FileDoesNotExistException | InvalidFileSizeException | InvalidPathException
          | IOException e) {
        LOG.warn("FAILED TO SYNC METADATA: {}", e.getMessage(), e);
      } finally {
        // regardless of the outcome, remove the UfsStatus for this path from the cache
        mStatusCache.remove(path);
      }
    }
    LOG.info("TRACING - Synced {} paths", syncPathCount);
    mStatusCache.cancelAllPrefetch();
    return syncPathCount > 0;
  }

  private void syncInodeMetadata(LockedInodePath inodePath)
      throws InvalidPathException, AccessControlException, IOException, FileDoesNotExistException,
      FileAlreadyCompletedException, InvalidFileSizeException, BlockInfoException {
    if (!inodePath.fullPathExists()) {
      loadMetadataForPath(inodePath);
    }
    syncExistingInodeMetadata(inodePath);
  }

  /**
   * Sync inode metadata with the UFS state.
   *
   * This method expects the {@code inodePath} to already exist in the inode tree.
   */
  private void syncExistingInodeMetadata(LockedInodePath inodePath)
      throws AccessControlException, BlockInfoException, FileAlreadyCompletedException,
      FileDoesNotExistException, InvalidFileSizeException, InvalidPathException, IOException {
    if (inodePath.getLockPattern() != LockPattern.WRITE_EDGE && !mLoadOnly) {
      throw new RuntimeException(String.format(
          "syncExistingInodeMetadata was called on %s when only locked with %s. Load metadata"
          + " only was not specified.", inodePath.getUri(), inodePath.getLockPattern()));
    }

    // Set to true if the given inode was deleted.
    boolean deletedInode = false;
    // whether we need to load metadata for the current path
    boolean loadMetadata = mLoadOnly;
    boolean syncChildren = true;
    LOG.debug("Syncing inode metadata {}", inodePath.getUri());

    // The requested path already exists in Alluxio.
    Inode inode = inodePath.getInode();

    // if the lock pattern is WRITE_EDGE, then we can sync (update or delete). Otherwise, if it is
    // we can only load metadata.

    if (inodePath.getLockPattern() == LockPattern.WRITE_EDGE && !mLoadOnly) {
      if (inode instanceof InodeFile && !inode.asFile().isCompleted()) {
        // Do not sync an incomplete file, since the UFS file is expected to not exist.
        return;
      }

      Optional<Scoped> persistingLock = mInodeLockManager.tryAcquirePersistingLock(inode.getId());
      if (!persistingLock.isPresent()) {
        // Do not sync a file in the process of being persisted, since the UFS file is being
        // written.
        return;
      }
      persistingLock.get().close();

      UfsStatus cachedStatus = mStatusCache.getStatus(inodePath.getUri());
      MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
      AlluxioURI ufsUri = resolution.getUri();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        String ufsFingerprint;
        Fingerprint ufsFpParsed;
        if (cachedStatus == null) {
          // TODO(david): change the interface so that getFingerprint returns a parsed fingerprint
          ufsFingerprint = ufs.getFingerprint(ufsUri.toString());
          ufsFpParsed = Fingerprint.parse(ufsFingerprint);
        } else {
          Pair<AccessControlList, DefaultAccessControlList> aclPair =
              ufs.getAclPair(ufsUri.toString());

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
            SetAttributePOptions.Builder builder = SetAttributePOptions.newBuilder()
              .setMode(new Mode(mode).toProto());
            String owner = ufsFpParsed.getTag(Tag.OWNER);
            if (!owner.equals(Fingerprint.UNDERSCORE)) {
              // Only set owner if not empty
              builder.setOwner(owner);
            }
            String group = ufsFpParsed.getTag(Tag.GROUP);
            if (!group.equals(Fingerprint.UNDERSCORE)) {
              // Only set group if not empty
              builder.setGroup(group);
            }
            mFsMaster.setAttributeSingleFile(mRpcContext, inodePath, false, opTimeMs,
                SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
                    .setOwner(ufsFpParsed.getTag(Fingerprint.Tag.OWNER))
                    .setGroup(ufsFpParsed.getTag(Fingerprint.Tag.GROUP))
                    .setMode(new Mode(mode).toProto())).setUfsFingerprint(ufsFingerprint));
          }
        }

        if (syncPlan.toDelete()) {
          deletedInode = true;
          try {
            // The options for deleting.
            DeleteContext syncDeleteContext = DeleteContext.mergeFrom(
                DeletePOptions.newBuilder()
                .setRecursive(true)
                .setAlluxioOnly(true)
                .setUnchecked(true));
            mFsMaster.deleteInternal(mRpcContext, inodePath, syncDeleteContext);
          } catch (DirectoryNotEmptyException | IOException e) {
            // Should not happen, since it is an unchecked delete.
            LOG.error("Unexpected error for unchecked delete.", e);
          }
        }

        if (syncPlan.toLoadMetadata()) {
          loadMetadata = true;
        }

        syncChildren = syncPlan.toSyncChildren();
      }
    }

    syncChildren = syncChildren
        && inode.isDirectory()
        && mDescendantType != DescendantType.NONE;

    Map<String, Inode> inodeChildren = new HashMap<>();
    if (syncChildren) {
      // maps children name to inode
      mInodeStore.getChildren(inode.asDirectory())
          .forEach(child -> inodeChildren.put(child.getName(), child));

      // Fetch and populate children into the cache
      Collection<UfsStatus> listStatus = mStatusCache
          .fetchChildrenIfAbsent(inodePath.getUri(), mMountTable);
      // Iterate over UFS listings and process UFS children.
      if (listStatus != null) {
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
    // If the inode was deleted in the previous sync step, we need to remove the inode from the
    // locked path
    if (deletedInode) {
      inodePath.removeLastInode();
    }

    // load metadata if necessary.
    if (loadMetadata) {
      loadMetadataForPath(inodePath);
    }
    mUfsSyncPathCache.notifySyncedPath(inodePath.getUri().getPath(), DescendantType.ONE);

    if (syncChildren) {
      // Iterate over Alluxio children and process persisted children.
      mInodeStore.getChildren(inode.asDirectory()).forEach(childInode -> {
        // If we are only loading non-existing metadata, then don't process any child which
        // was already in the tree, unless it is a directory, in which case, we might need to load
        // its children.
        if (mLoadOnly && inodeChildren.containsKey(childInode.getName()) && childInode.isFile()) {
          return;
        }
        // If we're performing a recursive sync, add each child of our current Inode to the queue
        AlluxioURI child = inodePath.getUri().joinUnsafe(childInode.getName());
        mSyncMetadataQ.add(child);
        // This asynchronously schedules a job to pre-fetch the statuses into the cache.
        if (childInode.isDirectory()) {
          mStatusCache.prefetchChildren(child, mMountTable);
        }
      });
    }
  }

  private void loadMetadataForPath(LockedInodePath inodePath)
      throws InvalidPathException, AccessControlException, IOException, FileDoesNotExistException,
      FileAlreadyCompletedException, InvalidFileSizeException, BlockInfoException {

    UfsStatus status = mStatusCache.getStatus(inodePath.getUri());
    LoadMetadataContext ctx = LoadMetadataContext.mergeFrom(
        LoadMetadataPOptions.newBuilder()
            .setCreateAncestors(true)
            .setLoadDescendantType(GrpcUtils.toProto(mDescendantType)))
        .setUfsStatus(status);
    loadMetadata(inodePath, ctx);
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
        Inode inode = inodePath.getInode();
        if (inode.isFile()) {
          throw new IllegalArgumentException(String.format(
              "load metadata cannot be called on a file if no ufs "
                  + "status is present in the context. %s", inodePath.getUri()));
        }

        mInodeTree.setDirectChildrenLoaded(mRpcContext, inode.asDirectory());
        return;
      }
      boolean isFile;
      if (context.getUfsStatus() != null) {
        isFile = context.getUfsStatus().isFile();
      } else {
        isFile = ufs.isFile(ufsUri.toString());
      }
      if (isFile) {
        loadFileMetadataInternal(mRpcContext, inodePath, resolution, context, mFsMaster);
      } else {
        loadDirectoryMetadata(mRpcContext, inodePath, context, mMountTable, mFsMaster);

        // now load all children if required
        LoadDescendantPType type = context.getOptions().getLoadDescendantType();
        if (type != LoadDescendantPType.NONE) {
          Collection<UfsStatus> children = mStatusCache.fetchChildrenIfAbsent(inodePath.getUri(),
              mMountTable);
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
            LoadMetadataContext loadMetadataContext =
                LoadMetadataContext.mergeFrom(LoadMetadataPOptions.newBuilder()
                    .setLoadDescendantType(LoadDescendantPType.NONE)
                    .setCreateAncestors(false))
                .setUfsStatus(childStatus);
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
  static void loadFileMetadataInternal(RpcContext rpcContext, LockedInodePath inodePath,
      MountTable.Resolution resolution, LoadMetadataContext context,
      DefaultFileSystemMaster fsMaster)
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

      if (fsMaster.isAclEnabled()) {
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
      fsMaster.createFileInternal(rpcContext, writeLockedPath, createFileContext);
      CompleteFileContext completeContext =
          CompleteFileContext.mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(ufsLength))
              .setUfsStatus(context.getUfsStatus());
      if (ufsLastModified != null) {
        completeContext.setOperationTimeMs(ufsLastModified);
      }
      fsMaster.completeFileInternal(rpcContext, writeLockedPath, completeContext);
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
  static void loadDirectoryMetadata(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataContext context, MountTable mountTable, DefaultFileSystemMaster fsMaster)
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
    createDirectoryContext.setMountPoint(mountTable.isMountPoint(inodePath.getUri()));
    createDirectoryContext.setMetadataLoad(true);
    createDirectoryContext.setWriteType(WriteType.THROUGH);
    MountTable.Resolution resolution = mountTable.resolve(inodePath.getUri());

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

    try (LockedInodePath writeLockedPath = inodePath.lockFinalEdgeWrite()) {
      fsMaster.createDirectoryInternal(rpcContext, writeLockedPath, createDirectoryContext);
    } catch (FileAlreadyExistsException e) {
      // This may occur if a thread created or loaded the directory before we got the write lock.
      // The directory already exists, so nothing needs to be loaded.
    }
    // Re-traverse the path to pick up any newly created inodes.
    inodePath.traverse();
  }
}
