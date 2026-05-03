package ru.hse.drip.systems.distributed.homework.i3.cli;

import java.util.HashMap;
import java.util.Map;

public class CliParser {

    public Parsed parse(String line) {
        String[] p = line.trim().split("\\s+");
        if (p.length == 0) return null;

        Parsed r = new Parsed();
        r.cmd = p[0];

        int i = 1;
        while (i < p.length && !p[i].startsWith("--")) {
            r.args.add(p[i]);
            i++;
        }

        while (i < p.length) {
            String opt = p[i++];
            if (i >= p.length) break;
            String val = p[i++];
            r.opts.put(opt, val);
        }
        return r;
    }

    public static class Parsed {
        public String cmd;
        public java.util.List<String> args = new java.util.ArrayList<>();
        public Map<String,String> opts = new HashMap<>();
    }
}
