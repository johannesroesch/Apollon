/*
 *    Copyright 2021 Johannes Roesch
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package io.github.johannesroesch.apollon.embedded;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.stream.IntStream;

public class PortFinder {

    private static final int MAX_TRIES = 100;
    private static final int MIN_PORT_NUMBER = 1025;
    private static final int MAX_PORT_NUMBER = 65534;

    private PortFinder() {

    }

    public static int randomAvailable() {
        return findAvailableBetween(MIN_PORT_NUMBER, MAX_PORT_NUMBER);
    }

    public static int findFirstAvailableBetween(int startInclusive, int endExclusive) {
        return IntStream.range(startInclusive, endExclusive)
                .filter(port -> PortFinder.isTcpPortAvailable("localhost", port))
                .filter(port -> PortFinder.isTcpPortAvailable("127.0.0.1", port))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no available port found between " + startInclusive + " and " + endExclusive + "."));
    }

    public static Integer findAvailableBetween(int start, int end) {
        int tested = randomBetween(start, end);

        for (int i = 0; i < MAX_TRIES; i++) {
            if (isAvailable(tested)) {
                return tested;
            }
            tested = randomBetween(start, end);
        }
        throw new IllegalStateException("no available port found between " + start + " and " + end + " after "
                + MAX_TRIES + "tries");
    }

    /**
     * http://stackoverflow.com/questions/434718/sockets-discover-port-
     * availability-using-java
     *
     * @deprecated
     */
    @Deprecated
    public static boolean isAvailable(int port) {
        if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
            return false;
        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException ignore) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }

    /**
     * https://stackoverflow.com/questions/434718/sockets-discover-port-availability-using-java
     */
    public static boolean isTcpPortAvailable(String hostname, int port) {
        try (ServerSocket serverSocket = new ServerSocket()) {
            // setReuseAddress(false) is required only on OSX,
            // otherwise the code will not work correctly on that platform
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName(hostname), port), 1);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private static int randomBetween(int start, int end) {
        return start + (int) (Math.random() * ((end - start) + 1));
    }
}
