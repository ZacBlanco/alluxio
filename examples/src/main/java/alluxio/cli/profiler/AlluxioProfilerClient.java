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

package alluxio.cli.profiler;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * Profiler client for Alluxio.
 */
public class AlluxioProfilerClient extends ProfilerClient {


  private final FileSystem mClient;

  public AlluxioProfilerClient() {
    mClient = FileSystem.Factory.get();
  }

  @Override
  public void delete(String rawPath) throws IOException {
    AlluxioURI path = new AlluxioURI(rawPath);
    try {
      if (!sDryRun) {
        mClient.delete(path,
            DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).build());
      } else {
        System.out.println("Delete: " + path);
      }
    } catch (FileDoesNotExistException e) {
      // ok
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void createFile(String rawPath, long fileSize) throws IOException {

    AlluxioURI path = new AlluxioURI(rawPath);
    if (!sDryRun) {
      try (FileOutStream stream = mClient.createFile(path,
          CreateFilePOptions.newBuilder().build())) {
        writeOutput(stream, fileSize);
      } catch (FileDoesNotExistException e) {
        // ok
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
    } else {
      System.out.println("Create file: " + rawPath);
    }
  }

  @Override
  public void createDir(String rawPath) throws IOException {
    AlluxioURI path = new AlluxioURI(rawPath);
    if (!sDryRun) {
      try {
        mClient.createDirectory(path,
            CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
      } catch (FileDoesNotExistException e) {
        // ok
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
    } else {
      System.out.println("Create directory: " + rawPath);
    }
  }

  @Override
  public void list(String rawPath) throws IOException {
    if (!sDryRun) {
      try {
        mClient.listStatus(new AlluxioURI(rawPath), ListStatusPOptions.newBuilder()
            .setRecursive(true)
            .build());
      } catch (FileDoesNotExistException e) {
        // ok
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
    } else {
      System.out.println("List path: " + rawPath);
    }
  }
}
