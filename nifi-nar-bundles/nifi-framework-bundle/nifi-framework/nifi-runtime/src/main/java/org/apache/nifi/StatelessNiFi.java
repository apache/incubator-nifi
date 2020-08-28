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
package org.apache.nifi;

import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarUnpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

public class StatelessNiFi {
    private static final Logger logger = LoggerFactory.getLogger(StatelessNiFi.class);

    public static final String PROGRAM_CLASS_NAME = "org.apache.nifi.stateless.runtimes.Program";

    public static final String EXTRACT_NARS = "ExtractNars";

    public static void main(final String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        String nifi_home = System.getenv("NIFI_HOME");
        if (nifi_home == null || nifi_home.equals("")) {
            nifi_home = ".";
        }

        final File libDir = new File(nifi_home + "/lib");
        final File narWorkingDirectory = new File(nifi_home + "/work/stateless-nars");
        final File frameworkWorkingDirectory = new File(narWorkingDirectory + "/framework");
        final File extensionsWorkingDirectory = new File(narWorkingDirectory + "/extensions");

        if (args.length >= 1 && args[0].equals(EXTRACT_NARS)) {
            if (!libDir.exists()) {
                System.out.println("Specified lib directory <" + libDir + "> does not exist");
                return;
            }

            final File[] narFiles = libDir.listFiles(file -> file.getName().endsWith(".nar"));
            if (narFiles == null) {
                System.out.println("Could not obtain listing of lib directory <" + libDir + ">");
                return;
            }

            if (!narWorkingDirectory.exists() && !narWorkingDirectory.mkdirs()) {
                throw new IOException("Could not create NAR working directory <" + narWorkingDirectory + ">");
            }

            logger.info("Unpacking {} NARs", narFiles.length);
            final long startUnpack = System.nanoTime();
            for (final File narFile : narFiles) {
                if (narFile.getName().startsWith("nifi-framework")) {
                    NarUnpacker.unpackNar(narFile, frameworkWorkingDirectory);
                } else {
                    NarUnpacker.unpackNar(narFile, extensionsWorkingDirectory);
                }
            }

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startUnpack);
            logger.info("Finished unpacking {} NARs in {} millis", narFiles.length, millis);

            System.exit(0);
        }

        final File[] frameworkNars = frameworkWorkingDirectory.listFiles(file -> file.getName().startsWith("nifi-framework"));
        if (frameworkNars == null) {
            throw new FileNotFoundException("Could not find core stateless dependencies in the working directory <" + frameworkWorkingDirectory + ">");
        }

        final File[] extensionNars = extensionsWorkingDirectory.listFiles(file -> file.getName().startsWith("nifi-"));
        if (extensionNars == null) {
            throw new FileNotFoundException("Could not find core stateless dependencies in the working directory <" + extensionsWorkingDirectory + ">");
        }


        final ClassLoader rootClassLoader = Thread.currentThread().getContextClassLoader();
        NarClassLoaders narClassLoaders = NarClassLoadersHolder.getInstance();
        narClassLoaders.init(rootClassLoader, frameworkWorkingDirectory,extensionsWorkingDirectory);
        final ClassLoader frameworkClassLoader = narClassLoaders.getFrameworkBundle().getClassLoader();

        final Class<?> programClass = Class.forName(PROGRAM_CLASS_NAME, true, frameworkClassLoader);
        final Method launchMethod = programClass.getMethod("launch", String[].class, ClassLoader.class, File.class);
        launchMethod.setAccessible(true);
        launchMethod.invoke(null, args, rootClassLoader, narWorkingDirectory);
    }

}
