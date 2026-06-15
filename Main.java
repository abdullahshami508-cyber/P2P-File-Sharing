import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;

class PeerInfo {
    private String peerId;
    private InetSocketAddress address;
    private long bandwidth;

    public PeerInfo(String peerId, InetSocketAddress address, long bandwidth) {
        this.peerId = peerId;
        this.address = address;
        this.bandwidth = bandwidth;
    }

    public String getPeerId() { return peerId; }
    public InetSocketAddress getAddress() { return address; }
    public long getBandwidth() { return bandwidth; }

    @Override
    public String toString() {
        return peerId + "@" + address;
    }
}

class FileChunk {
    private int index;
    private byte[] data;

    public FileChunk(int index, byte[] data) {
        this.index = index;
        this.data = data;
    }

    public int getIndex() { return index; }
    public byte[] getData() { return data; }
}

class PeerNode {
    private String peerId;
    private Set<PeerNode> neighbors;

    public PeerNode(String peerId) {
        this.peerId = peerId;
        this.neighbors = new HashSet<>();
    }

    public String getPeerId() { return peerId; }
    public Set<PeerNode> getNeighbors() { return neighbors; }
    public void addNeighbor(PeerNode neighbor) { neighbors.add(neighbor); }
}

class PeerGraph {
    private Map<String, PeerNode> nodeMap = new ConcurrentHashMap<>();

    public void addPeer(String peerId) {
        nodeMap.putIfAbsent(peerId, new PeerNode(peerId));
    }

    public void addConnection(String fromId, String toId) {
        PeerNode from = nodeMap.get(fromId);
        PeerNode to = nodeMap.get(toId);
        if (from != null && to != null) {
            from.addNeighbor(to);
            to.addNeighbor(from);
        }
    }

    public Set<String> searchFile(String fileName, Map<String, Set<String>> fileIndex) {
        Set<String> result = new HashSet<>();
        if (nodeMap.isEmpty()) return result;

        Stack<PeerNode> stack = new Stack<>();
        Set<String> visited = new HashSet<>();
        PeerNode start = nodeMap.values().iterator().next();
        stack.push(start);
        visited.add(start.getPeerId());

        while (!stack.isEmpty()) {
            PeerNode current = stack.pop();
            Set<String> files = fileIndex.get(current.getPeerId());
            if (files != null && files.contains(fileName)) {
                result.add(current.getPeerId());
            }
            for (PeerNode neighbor : current.getNeighbors()) {
                if (!visited.contains(neighbor.getPeerId())) {
                    visited.add(neighbor.getPeerId());
                    stack.push(neighbor);
                }
            }
        }
        return result;
    }
}

class FileTransferManager {
    private static final int CHUNK_SIZE = 1024;

    public static void downloadFile(String fileName, List<PeerInfo> availablePeers, String destFolder, JTextArea logArea) throws Exception {
        PriorityQueue<PeerInfo> peerQueue = new PriorityQueue<>(
                (p1, p2) -> Long.compare(p2.getBandwidth(), p1.getBandwidth())
        );
        peerQueue.addAll(availablePeers);
        PeerInfo bestPeer = peerQueue.poll();
        if (bestPeer == null) {
            appendLog("No peer available for download.", logArea);
            return;
        }
        appendLog("Downloading from best peer: " + bestPeer, logArea);

        try (Socket socket = new Socket(bestPeer.getAddress().getAddress(), bestPeer.getAddress().getPort())) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF("GET_SIZE:" + fileName);
            DataInputStream in = new DataInputStream(socket.getInputStream());
            long fileSize = in.readLong();
            if (fileSize <= 0) {
                appendLog("File not found on peer.", logArea);
                return;
            }
            int totalChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);
            TreeMap<Integer, byte[]> chunkMap = new TreeMap<>();
            Set<Integer> receivedChunks = new HashSet<>();

            ExecutorService executor = Executors.newFixedThreadPool(5);
            for (int i = 0; i < totalChunks; i++) {
                final int idx = i;
                executor.submit(() -> {
                    try {
                        downloadChunk(bestPeer, fileName, idx, chunkMap, receivedChunks, logArea);
                    } catch (Exception e) {
                        appendLog("Chunk " + idx + " download failed: " + e.getMessage(), logArea);
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);

            if (receivedChunks.size() == totalChunks) {
                String outputPath = destFolder + "/" + fileName;
                try (FileOutputStream fos = new FileOutputStream(outputPath)) {
                    for (byte[] data : chunkMap.values()) {
                        fos.write(data);
                    }
                }
                appendLog("File downloaded successfully: " + outputPath, logArea);
            } else {
                appendLog("Missing chunks. Download incomplete.", logArea);
            }
        }
    }

    private static void downloadChunk(PeerInfo peer, String fileName, int index,
                                      TreeMap<Integer, byte[]> chunkMap, Set<Integer> received, JTextArea logArea) throws Exception {
        try (Socket socket = new Socket(peer.getAddress().getAddress(), peer.getAddress().getPort())) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF("GET_CHUNK:" + fileName + ":" + index);
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int len = in.readInt();
            if (len > 0) {
                byte[] data = new byte[len];
                in.readFully(data);
                synchronized (chunkMap) {
                    chunkMap.put(index, data);
                    received.add(index);
                }
                appendLog("Chunk " + index + " downloaded.", logArea);
            }
        }
    }

    private static void appendLog(String msg, JTextArea logArea) {
        if (logArea != null) {
            SwingUtilities.invokeLater(() -> logArea.append(msg + "\n"));
        } else {
            System.out.println(msg);
        }
    }
}

class Peer {
    private String peerId;
    private int port;
    private PeerGraph graph;
    private Map<String, Set<String>> fileIndex;
    private Map<String, Set<String>> localFiles;
    private Map<String, PeerInfo> peerRegistry;
    private ServerSocket serverSocket;
    private ExecutorService threadPool;
    private JTextArea logArea;

    public Peer(String peerId, int port, JTextArea logArea) {
        this.peerId = peerId;
        this.port = port;
        this.logArea = logArea;
        this.graph = new PeerGraph();
        this.fileIndex = new ConcurrentHashMap<>();
        this.localFiles = new ConcurrentHashMap<>();
        this.peerRegistry = new ConcurrentHashMap<>();
        this.threadPool = Executors.newCachedThreadPool();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        appendLog("Peer " + peerId + " listening on port " + port);
        graph.addPeer(peerId);
        peerRegistry.put(peerId, new PeerInfo(peerId, new InetSocketAddress("localhost", port), 1000 + new Random().nextInt(9000)));
        new Thread(() -> {
            while (true) {
                try {
                    Socket client = serverSocket.accept();
                    threadPool.submit(() -> handleClient(client));
                } catch (IOException e) {
                    break;
                }
            }
        }).start();
    }

    private void handleClient(Socket socket) {
        try (DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            String request = in.readUTF();
            if (request.startsWith("GET_SIZE:")) {
                String fileName = request.substring(9);
                Path filePath = getLocalFilePath(fileName);
                if (filePath != null && Files.exists(filePath)) {
                    out.writeLong(Files.size(filePath));
                } else {
                    out.writeLong(-1);
                }
            } else if (request.startsWith("GET_CHUNK:")) {
                String[] parts = request.split(":");
                String fileName = parts[1];
                int chunkIndex = Integer.parseInt(parts[2]);
                sendChunk(fileName, chunkIndex, out);
            } else if (request.startsWith("SEARCH:")) {
                String fileName = request.substring(7);
                Set<String> peers = graph.searchFile(fileName, fileIndex);
                out.writeInt(peers.size());
                for (String pid : peers) {
                    out.writeUTF(pid);
                }
            } else if (request.startsWith("JOIN:")) {
                String[] parts = request.split(":");
                String newPeerId = parts[1];
                int newPort = Integer.parseInt(parts[2]);
                long bw = Long.parseLong(parts[3]);
                PeerInfo info = new PeerInfo(newPeerId, new InetSocketAddress("localhost", newPort), bw);
                peerRegistry.put(newPeerId, info);
                graph.addPeer(newPeerId);
                graph.addConnection(peerId, newPeerId);
                out.writeUTF("OK");
            }
        } catch (Exception e) {
            appendLog("Error handling client: " + e.getMessage());
        }
    }

    private Path getLocalFilePath(String fileName) {
        Path sharedDir = Paths.get("shared_" + peerId);
        Path filePath = sharedDir.resolve(fileName);
        return Files.exists(filePath) ? filePath : null;
    }

    private void sendChunk(String fileName, int chunkIndex, DataOutputStream out) throws IOException {
        Path filePath = getLocalFilePath(fileName);
        if (filePath == null) {
            out.writeInt(0);
            return;
        }
        long size = Files.size(filePath);
        int chunkSize = 1024;
        long start = (long) chunkIndex * chunkSize;
        if (start >= size) {
            out.writeInt(0);
            return;
        }
        int len = (int) Math.min(chunkSize, size - start);
        byte[] data = new byte[len];
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            raf.seek(start);
            raf.readFully(data);
        }
        out.writeInt(len);
        out.write(data);
    }

    public void shareFolder(String folderPath) throws IOException {
        Path folder = Paths.get(folderPath);
        if (!Files.exists(folder)) Files.createDirectories(folder);
        Set<String> files = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder)) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry)) {
                    files.add(entry.getFileName().toString());
                }
            }
        }
        localFiles.put(peerId, files);
        fileIndex.put(peerId, files);
        appendLog("Peer " + peerId + " shares " + files.size() + " files.");
    }

    public void joinNetwork(String bootstrapHost, int bootstrapPort) throws IOException {
        try (Socket socket = new Socket(bootstrapHost, bootstrapPort)) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.writeUTF("JOIN:" + peerId + ":" + port + ":" + peerRegistry.get(peerId).getBandwidth());
            String response = in.readUTF();
            if ("OK".equals(response)) {
                appendLog("Peer " + peerId + " joined network via bootstrap.");
            }
        }
    }

    public Set<String> searchFile(String fileName) throws IOException {
        Set<String> result = new HashSet<>();
        for (PeerInfo info : peerRegistry.values()) {
            try (Socket socket = new Socket(info.getAddress().getAddress(), info.getAddress().getPort())) {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF("SEARCH:" + fileName);
                DataInputStream in = new DataInputStream(socket.getInputStream());
                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    result.add(in.readUTF());
                }
            } catch (Exception ignored) {}
        }
        return result;
    }

    public void downloadFile(String fileName, String destFolder) throws Exception {
        Set<String> peerIds = searchFile(fileName);
        if (peerIds.isEmpty()) {
            appendLog("File not found in network.");
            return;
        }
        List<PeerInfo> peers = new ArrayList<>();
        for (String pid : peerIds) {
            PeerInfo info = peerRegistry.get(pid);
            if (info != null) peers.add(info);
        }
        FileTransferManager.downloadFile(fileName, peers, destFolder, logArea);
    }

    private void appendLog(String msg) {
        if (logArea != null) {
            SwingUtilities.invokeLater(() -> logArea.append(msg + "\n"));
        } else {
            System.out.println(msg);
        }
    }
}

public class Main extends JFrame {
    private Peer peer;
    private JTextArea logArea;
    private JTextField shareFolderField;
    private JTextField searchField;
    private DefaultListModel<String> resultModel;

    public Main() {
        setTitle("P2P File Sharing System");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setSize(700, 600);
        setLayout(new BorderLayout());

        JPanel topPanel = new JPanel(new GridLayout(3, 2, 5, 5));
        topPanel.setBorder(BorderFactory.createTitledBorder("Peer Configuration"));

        JTextField peerIdField = new JTextField("peer1");
        JTextField portField = new JTextField("9001");
        shareFolderField = new JTextField("shared_peer1");
        JButton startButton = new JButton("Start Peer");
        JButton shareButton = new JButton("Share Folder");
        JButton joinButton = new JButton("Join Network");

        topPanel.add(new JLabel("Peer ID:"));
        topPanel.add(peerIdField);
        topPanel.add(new JLabel("Port:"));
        topPanel.add(portField);
        topPanel.add(new JLabel("Share Folder:"));
        topPanel.add(shareFolderField);

        JPanel buttonPanel = new JPanel();
        buttonPanel.add(startButton);
        buttonPanel.add(shareButton);
        buttonPanel.add(joinButton);

        JPanel centerPanel = new JPanel(new BorderLayout());
        centerPanel.setBorder(BorderFactory.createTitledBorder("File Search"));
        JPanel searchPanel = new JPanel(new BorderLayout());
        searchField = new JTextField();
        JButton searchButton = new JButton("Search");
        searchPanel.add(searchField, BorderLayout.CENTER);
        searchPanel.add(searchButton, BorderLayout.EAST);
        centerPanel.add(searchPanel, BorderLayout.NORTH);

        resultModel = new DefaultListModel<>();
        JList<String> resultList = new JList<>(resultModel);
        resultList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        JScrollPane resultScroll = new JScrollPane(resultList);
        centerPanel.add(resultScroll, BorderLayout.CENTER);

        JButton downloadButton = new JButton("Download Selected File");
        centerPanel.add(downloadButton, BorderLayout.SOUTH);

        logArea = new JTextArea();
        logArea.setEditable(false);
        JScrollPane logScroll = new JScrollPane(logArea);
        logScroll.setBorder(BorderFactory.createTitledBorder("Log"));

        add(topPanel, BorderLayout.NORTH);
        add(buttonPanel, BorderLayout.CENTER);
        add(centerPanel, BorderLayout.WEST);
        add(logScroll, BorderLayout.SOUTH);

        startButton.addActionListener(e -> {
            try {
                String pid = peerIdField.getText().trim();
                int p = Integer.parseInt(portField.getText().trim());
                peer = new Peer(pid, p, logArea);
                peer.start();
                logArea.append("Peer started.\n");
            } catch (Exception ex) {
                logArea.append("Error: " + ex.getMessage() + "\n");
            }
        });

        shareButton.addActionListener(e -> {
            if (peer == null) {
                logArea.append("Start peer first.\n");
                return;
            }
            try {
                peer.shareFolder(shareFolderField.getText().trim());
            } catch (Exception ex) {
                logArea.append("Error sharing: " + ex.getMessage() + "\n");
            }
        });

        joinButton.addActionListener(e -> {
            if (peer == null) {
                logArea.append("Start peer first.\n");
                return;
            }
            try {
                peer.joinNetwork("localhost", 9000);
            } catch (Exception ex) {
                logArea.append("Join error: " + ex.getMessage() + "\n");
            }
        });

        searchButton.addActionListener(e -> {
            if (peer == null) {
                logArea.append("Start peer first.\n");
                return;
            }
            String file = searchField.getText().trim();
            if (file.isEmpty()) return;
            new Thread(() -> {
                try {
                    Set<String> peers = peer.searchFile(file);
                    SwingUtilities.invokeLater(() -> {
                        resultModel.clear();
                        if (peers.isEmpty()) {
                            resultModel.addElement("No peers found.");
                        } else {
                            for (String p : peers) {
                                resultModel.addElement(p);
                            }
                        }
                        logArea.append("Search completed for " + file + "\n");
                    });
                } catch (Exception ex) {
                    logArea.append("Search error: " + ex.getMessage() + "\n");
                }
            }).start();
        });

        downloadButton.addActionListener(e -> {
            if (peer == null) {
                logArea.append("Start peer first.\n");
                return;
            }
            String selected = resultList.getSelectedValue();
            if (selected == null || selected.equals("No peers found.")) {
                logArea.append("Select a peer from search results.\n");
                return;
            }
            String fileName = searchField.getText().trim();
            new Thread(() -> {
                try {
                    peer.downloadFile(fileName, "downloads");
                } catch (Exception ex) {
                    logArea.append("Download error: " + ex.getMessage() + "\n");
                }
            }).start();
        });
    }

    public static void main(String[] args) throws Exception {
        Files.createDirectories(Paths.get("shared_peer1"));
        Files.createDirectories(Paths.get("shared_peer2"));
        Files.createDirectories(Paths.get("shared_peer3"));
        Files.createDirectories(Paths.get("downloads"));

        new Thread(() -> {
            try {
                Peer bootstrap = new Peer("bootstrap", 9000, null);
                bootstrap.start();
                Peer peer2 = new Peer("peer2", 9002, null);
                peer2.start();
                peer2.shareFolder("shared_peer2");
                peer2.joinNetwork("localhost", 9000);
                Peer peer3 = new Peer("peer3", 9003, null);
                peer3.start();
                peer3.shareFolder("shared_peer3");
                peer3.joinNetwork("localhost", 9000);
                String content = "Hello from peer2! This is a test file for P2P transfer.";
                Files.write(Paths.get("shared_peer2/sample.txt"), content.getBytes());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        SwingUtilities.invokeLater(() -> new Main().setVisible(true));
    }
}
