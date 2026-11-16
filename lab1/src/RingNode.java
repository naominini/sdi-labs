import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * Sisteme distribuite - Laborator 1 (SCHELET - de completat)
 * Nod dintr-un inel TCP. Fiecare nod este simultan:
 * - server: ascultă pe portul propriu mesajele venite de la predecesor;
 * - client: ține o conexiune deschisă către succesor și îi trimite mesajele.
 * La primirea unui mesaj, nodul își adaugă identificatorul în traseu și îl trimite
 mai departe.
 * Inițiatorul (opțiunea --start N) lansează N ture și măsoară timpul dus-întors.
 *
 * Utilizare:
 * java RingNode.java <id> <port> <host_succesor> <port_succesor> [--start N] [--
 delay ms] [--timeout ms]
 *
 * Format mesaj (o linie de text): origine;număr_tură;traseu ex. P1;2;P1>P2>P3
 *
 * Completați cele 5 locuri marcate cu TODO. Restul codului (argumente, logare,
 * măsurarea RTT, timeout-ul inițiatorului) este deja scris.
 */
public class RingNode {
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private final String id;
    private final int port;
    private final String nextHost;
    private final int nextPort;
    private int rounds = 0; // > 0 doar pentru inițiator
    private long delayMs = 0; // întârziere artificială înainte de retransmitere
    private long timeoutMs = 5000; // după cât timp inițiatorul consideră o tură "suspectă"
    private Socket nextSocket; // conexiunea către succesor (null = nedeschisă încă)
    private PrintWriter toNext; // fluxul de scriere pe nextSocket
    private final Map<Integer, Long> inFlight = new ConcurrentHashMap<>(); // tură -> momentul trimiterii
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor();
    public RingNode(String id, int port, String nextHost, int nextPort) {
        this.id = id;
        this.port = port;
        this.nextHost = nextHost;
        this.nextPort = nextPort;
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("utilizare: java RingNode.java <id> <port> <host_succesor> <port_succesor>"
                    + " [--start N] [--delay ms] [--timeout ms]");
            System.exit(1);
        }
        RingNode node = new RingNode(args[0], Integer.parseInt(args[1]), args[2],
                Integer.parseInt(args[3]));
        for (int i = 4; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--start" -> node.rounds = Integer.parseInt(args[i + 1]);
                case "--delay" -> node.delayMs = Long.parseLong(args[i + 1]);
                case "--timeout" -> node.timeoutMs = Long.parseLong(args[i + 1]);
                default -> throw new IllegalArgumentException("opțiune necunoscută: "
                        + args[i]);
            }
        }
        node.run();
    }
    void run() throws IOException {
        ServerSocket server = new ServerSocket(port);
        log("pornit; ascult pe portul " + port + ", succesor " + nextHost + ":" +
                nextPort);
        if (rounds > 0) {
            // inițiatorul lansează prima tură într-un fir separat, ca serverul să poată primi mesaje
            new Thread(() -> launchRound(1), "initiator").start();
        }
        // TODO 1 - server concurent:
        while (true) {
            Socket socket = server.accept();
            new Thread(() -> listen(socket), "din-" + socket.getPort()).start();
        }
    }
    /** Citește mesajele venite pe o conexiune de la predecesor. */
    private void listen(Socket socket) {
        String who = socket.getRemoteSocketAddress().toString();
        log("predecesor conectat: " + who);
        // TODO 2 - citirea mesajelor:
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                onMessage(line);
            }
        } catch (IOException e) {
            log("eroare la citire de la " + who + ": " + e.getMessage());
        }
        log("predecesorul " + who + " a închis conexiunea");
    }
    private void onMessage(String line) {
        String[] parts = line.split(";", 3);
        if (parts.length != 3) {
            log("mesaj invalid ignorat: " + line);
            return;
        }
        String origin = parts[0];
        int round = Integer.parseInt(parts[1]);
        String trace = parts[2];
        // TODO 3 - regula inelului:
        if (origin.equals(id)) {
            Long start = inFlight.remove(round);
            double rttMs = (start == null) ? -1 : (System.nanoTime() - start) / 1_000_000.0;
            log(String.format("tura %d s-a întors: %s (RTT %.3f ms)", round, trace + ">" + id, rttMs));
            if (round < rounds) {
                launchRound(round + 1);
            } else {
                log("toate cele " + rounds + " ture au fost finalizate");
            }
        } else {
            log("primit tura " + round + " de la " + origin + ": " + trace);
            sleep(delayMs);
            send(origin + ";" + round + ";" + trace + ">" + id);
        }
    }
    private void launchRound(int round) {
        inFlight.put(round, System.nanoTime());
        log("lansez tura " + round);
        send(id + ";" + round + ";" + id);
        timer.schedule(() -> {
            if (inFlight.containsKey(round)) {
                log("ATENȚIE: tura " + round + " nu s-a întors după " + timeoutMs
                                + " ms - pierdută, întârziată sau nod căzut? Nu putem distinge.");
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
    }
    /** Trimite o linie succesorului. */
    private synchronized void send(String msg) {
        // TODO 4 - trimiterea:
        for (int attempt = 0; attempt < 2; attempt++) {
            connectIfNeeded();
            toNext.println(msg);
            if (!toNext.checkError()) {
                return;
            }
            log("scriere eșuată, închid conexiunea");
            closeQuietly();
        }
        log("mesajul nu a putut fi trimis: " + msg);
    }
    /** Deschide conexiunea spre succesor, dacă nu este deja deschisă. */
    private void connectIfNeeded() {
        if (nextSocket != null && !nextSocket.isClosed()) {
            return;
        }
        // TODO 5 - conectarea cu reîncercări:
        long delay = 250;
        while (true) {
            try {
                nextSocket = new Socket(nextHost, nextPort);
                toNext = new PrintWriter(nextSocket.getOutputStream(), true, StandardCharsets.UTF_8);
                log("conectat la succesor " + nextHost + ":" + nextPort);
                return;
            } catch (IOException e) {
                log("succesorul nu ascultă încă, reîncerc în " + delay + " ms");
                sleep(delay);
                delay = Math.min(delay * 2, 8000);
            }
        }
    }
    private void closeQuietly() {
        try {
            if (nextSocket != null) nextSocket.close();
        } catch (IOException ignored) {
        }
        nextSocket = null;
    }
    private static void sleep(long ms) {
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    private void log(String msg) {
        System.out.printf("%s [%s|%s] %s%n", LocalTime.now().format(TIME), id,
                Thread.currentThread().getName(), msg);
    }
}
