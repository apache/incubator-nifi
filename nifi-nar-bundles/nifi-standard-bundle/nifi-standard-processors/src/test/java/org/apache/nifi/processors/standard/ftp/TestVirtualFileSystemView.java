/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard.ftp;

import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.nifi.processors.standard.ftp.filesystem.DefaultVirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFileSystemFactory;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualPath;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestVirtualFileSystemView {

    private FileSystemView fileSystemView;
    private static VirtualFileSystem fileSystem;

    @BeforeClass
    public static void setupVirtualFileSystem() {
        fileSystem = new DefaultVirtualFileSystem();
        fileSystem.mkdir(new VirtualPath("/Directory1"));
        fileSystem.mkdir(new VirtualPath("/Directory1/SubDirectory1"));
        fileSystem.mkdir(new VirtualPath("/Directory1/SubDirectory1/SubSubDirectory"));
        fileSystem.mkdir(new VirtualPath("/Directory1/SubDirectory2"));
        fileSystem.mkdir(new VirtualPath("/Directory2"));
        fileSystem.mkdir(new VirtualPath("/Directory2/SubDirectory3"));
        fileSystem.mkdir(new VirtualPath("/Directory2/SubDirectory4"));
    }

    @Before
    public void setup() throws FtpException {
        User user = createUser();
        FileSystemFactory fileSystemFactory = new VirtualFileSystemFactory(fileSystem);
        fileSystemView = fileSystemFactory.createFileSystemView(user);
    }

    @Test
    public void testInRootDirectory() throws FtpException {

        // WHEN
        // We do not change directories

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testTryToMakeRootDirectory() {

        // WHEN
        boolean directoryCreated = fileSystem.mkdir(VirtualFileSystem.ROOT);

        // THEN
        assertFalse(directoryCreated);
    }

    @Test
    public void testChangeToAnotherDirectory() throws FtpException {

        // WHEN
        fileSystemView.changeWorkingDirectory("/Directory1");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/Directory1");
    }

    @Test
    public void testChangeToRootDirectory() throws FtpException {

        // WHEN
        fileSystemView.changeWorkingDirectory("/");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testChangeToUnspecifiedDirectory() throws FtpException {

        // WHEN
        fileSystemView.changeWorkingDirectory("");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testChangeToSameDirectory() throws FtpException {

        // WHEN
        fileSystemView.changeWorkingDirectory(".");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testChangeToSameDirectoryNonRoot() throws FtpException {
        // GIVEN
        fileSystemView.changeWorkingDirectory("/Directory1");

        // WHEN
        fileSystemView.changeWorkingDirectory(".");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/Directory1");
    }

    @Test
    public void testChangeToParentDirectory() throws FtpException {
        // GIVEN
        fileSystemView.changeWorkingDirectory("/Directory1");

        // WHEN
        fileSystemView.changeWorkingDirectory("..");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testChangeToParentDirectoryNonRoot() throws FtpException {
        // GIVEN
        fileSystemView.changeWorkingDirectory("/Directory1");
        fileSystemView.changeWorkingDirectory("SubDirectory1");

        // WHEN
        fileSystemView.changeWorkingDirectory("..");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/Directory1");
    }

    @Test
    public void testChangeToNonExistentDirectory() throws FtpException {
        // GIVEN

        // WHEN
        boolean changeDirectoryResult = fileSystemView.changeWorkingDirectory("/Directory2/SubDirectory3/SubSubDirectory");

        // THEN
        assertFalse(changeDirectoryResult);
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testGetFileAbsolute() throws FtpException {
        // GIVEN
        fileSystemView.changeWorkingDirectory("/Directory1/SubDirectory1");

        // WHEN
        FtpFile file = fileSystemView.getFile("/Directory2/SubDirectory3");

        // THEN
        assertEquals("/Directory2/SubDirectory3", file.getAbsolutePath());
    }

    @Test
    public void testGetFileNonAbsolute() throws FtpException {
        // GIVEN
        fileSystemView.changeWorkingDirectory("/Directory1/SubDirectory1");

        // WHEN
        FtpFile file = fileSystemView.getFile("SubSubDirectory");

        // THEN
        assertEquals("/Directory1/SubDirectory1/SubSubDirectory", file.getAbsolutePath());
    }

    private User createUser() {
        BaseUser user = new BaseUser();
        user.setName("Username");
        user.setPassword("Password");
        user.setHomeDirectory("/abc/def");
        user.setAuthorities(Collections.singletonList(new WritePermission()));
        return user;
    }

    private void assertHomeDirectory(String expectedHomeDirectory) throws FtpException {
        FtpFile homeDirectory = fileSystemView.getHomeDirectory();
        assertEquals(expectedHomeDirectory, homeDirectory.getAbsolutePath());
    }

    private void assertCurrentDirectory(String expectedCurrentDirectory) throws FtpException {
        FtpFile currentDirectory = fileSystemView.getWorkingDirectory();
        assertEquals(expectedCurrentDirectory, currentDirectory.getAbsolutePath());
    }
}
