import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * Sisteme distribuite - Laborator 1
 * Server TCP de tip "ecou": trimite înapoi fiecare linie primită.
 *
 * Utilizare: java EchoServer.java <port> [iterativ|concurent]
 * iterativ - deservește clienții pe rând, în firul principal
 * concurent - fiecare client este deservit de un fir din pool (implicit)
 *
 * Comanda specială "SLEEP <ms>" face serverul să aștepte înainte de răspuns
 * (simulează un server lent - folosită în experimentele cu timeout).
 */
public class EchoServer {
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 5000;
        boolean concurent = args.length < 2 || !args[1].equalsIgnoreCase("iterativ");
        ExecutorService pool = Executors.newFixedThreadPool(16);
        try (ServerSocket server = new ServerSocket(port)) {
            log("ascult pe portul " + port + " (mod " + (concurent ? "concurent" :
                    "iterativ") + ")");
            while (true) {
                Socket client = server.accept(); // blochează până la o nouă conexiune
                if (concurent) {
                    pool.submit(() -> serve(client)); // un fir separat pentru  fiecare client
                } else {
                    serve(client); // următorul client așteaptă
                }
            }
        }
    }
    static void serve(Socket socket) {
        String who = socket.getRemoteSocketAddress().toString();
        log("client conectat: " + who);
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(),
                             StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true,
                     StandardCharsets.UTF_8)) {
            String line;
            while ((line = in.readLine()) != null) { // null = clientul a închis conexiunea
                log(who + " -> " + line);
                if (line.startsWith("SLEEP ")) {
                    Thread.sleep(Long.parseLong(line.substring(6).trim()));
                }
                out.println("ECHO " + line);
            }
        } catch (IOException | InterruptedException | NumberFormatException e) {
            log("eroare la " + who + ": " + e.getMessage());
        }
        log("client deconectat: " + who);
    }
    static void log(String msg) {
        System.out.printf("%s [%s] %s%n", LocalTime.now().format(TIME),
                Thread.currentThread().getName(), msg);
    }
}
