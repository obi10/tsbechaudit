package poc;

import poc.read.*;
import poc.write.*;

public class TSBenchAudit {

    public static void main(String[] args) {

        String type = args[0]; // Writes or Reads

        // writes
        if (type.equals("W")) {
            Writer w = new Writer();
            w.run();
        }

        // reads
        if (type.equals("R")) {
            Reader r = new Reader();
            r.run();
        }
    }
}
