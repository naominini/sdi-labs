import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
/**
 * Sisteme distribuite - Laborator 1
 * Client TCP pentru EchoServer: citește linii de la tastatură, le trimite
 * serverului și afișează răspunsul împreună cu timpul dus-întors (RTT).
 *
 * Utilizare: java EchoClient.java <host> <port> [timeout_ms]
 */
public class EchoClient {
    public static void main(String[] args) throws IOException {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5000;
        int timeoutMs = args.length > 2 ? Integer.parseInt(args[2]) : 3000;
        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(),
                             StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true,
                     StandardCharsets.UTF_8);
             BufferedReader keyboard = new BufferedReader(
                     new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            socket.setSoTimeout(timeoutMs); // readLine() nu mai blochează la nesfârșit
            System.out.println("conectat la " + socket.getRemoteSocketAddress()
                            + " (timeout " + timeoutMs + " ms). Scrieți mesaje; Ctrl+D pentru ieșire.");
                    String line;
            while ((line = keyboard.readLine()) != null) {
                long t0 = System.nanoTime();
                out.println(line);
                try {
                    String reply = in.readLine();
                    if (reply == null) {
                        System.out.println("serverul a închis conexiunea");
                        break;
                    }
                    double rttMs = (System.nanoTime() - t0) / 1e6;
                    System.out.printf("%s (RTT %.3f ms)%n", reply, rttMs);
                } catch (SocketTimeoutException e) {
                    System.out.println("niciun răspuns în " + timeoutMs
                                    + " ms - server lent, mesaj pierdut sau server căzut? Nu putem ști.");
                }
            }
        }
    }
}