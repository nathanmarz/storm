/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.commons.cli.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CLI {
    private static final Logger LOG = LoggerFactory.getLogger(CLI.class);
    private static class Opt {
        final String s;
        final String l;
        final Object defaultValue;
        final Parse parse;
        final Assoc assoc;
        public Opt(String s, String l, Object defaultValue, Parse parse, Assoc assoc) {
            this.s = s;
            this.l = l;
            this.defaultValue = defaultValue;
            this.parse = parse == null ? AS_STRING : parse;
            this.assoc = assoc == null ? LAST_WINS : assoc;
        }

        public Object process(Object current, String value) {
            return assoc.assoc(current, parse.parse(value));
        }
    }

    private static class Arg {
        final String name;
        final Parse parse;
        final Assoc assoc;
        public Arg(String name, Parse parse, Assoc assoc) {
            this.name = name;
            this.parse = parse == null ? AS_STRING : parse;
            this.assoc = assoc == null ? INTO_LIST : assoc;
        }

        public Object process(Object current, String value) {
            return assoc.assoc(current, parse.parse(value));
        }
    }

    public interface Parse {
        /**
         * Parse a String to the type you want it to be.
         * @param value the String to parse
         * @return the parsed value
         */
        public Object parse(String value);
    }

    public static final Parse AS_INT = new Parse() {
        @Override
        public Object parse(String value) {
            return Integer.valueOf(value);
        }
    };

    public static final Parse AS_STRING = new Parse() {
        @Override
        public Object parse(String value) {
            return value;
        }
    };

    public interface Assoc {
        /**
         * Associate a value into somthing else
         * @param current what to put value into, will be null if no values have been added yet.
         * @param value what to add
         * @return the result of combining the two
         */
        public Object assoc(Object current, Object value);
    }

    public static final Assoc LAST_WINS = new Assoc() {
        @Override
        public Object assoc(Object current, Object value) {
            return value;
        }
    };

    public static final Assoc FIRST_WINS = new Assoc() {
        @Override
        public Object assoc(Object current, Object value) {
            return current == null ? value : current;
        }
    };

    public static final Assoc INTO_LIST = new Assoc() {
        @Override
        public Object assoc(Object current, Object value) {
            if (current == null) {
                current = new ArrayList<Object>();
            }
            ((List<Object>)current).add(value);
            return current;
        }
    };

    public static class CLIBuilder {
        private final ArrayList<Opt> opts = new ArrayList<>();
        private final ArrayList<Arg> args = new ArrayList<>();

        public CLIBuilder opt(String s, String l, Object defaultValue) {
            return opt(s, l, defaultValue, null, null);
        }
 
        public CLIBuilder opt(String s, String l, Object defaultValue, Parse parse) {
            return opt(s, l, defaultValue, parse, null);
        }

        public CLIBuilder opt(String s, String l, Object defaultValue, Parse parse, Assoc assoc) {
            opts.add(new Opt(s, l, defaultValue, parse, assoc));
            return this;
        }

        public CLIBuilder arg(String name) {
            return arg(name, null, null);
        }

        public CLIBuilder arg(String name, Assoc assoc) {
            return arg(name, null, assoc);
        }
 
        public CLIBuilder arg(String name, Parse parse) {
            return arg(name, parse, null);
        }

        public CLIBuilder arg(String name, Parse parse, Assoc assoc) {
            args.add(new Arg(name, parse, assoc));
            return this;
        }

        public Map<String, Object> parse(String[] rawArgs) throws Exception {
            Options options = new Options();
            for (Opt opt: opts) {
                options.addOption(Option.builder(opt.s).longOpt(opt.l).hasArg().build());
            }
            DefaultParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, rawArgs);
            HashMap<String, Object> ret = new HashMap<>();
            for (Opt opt: opts) {
                Object current = null;
                for (String val: cl.getOptionValues(opt.s)) {
                    current = opt.process(current, val);
                }
                if (current == null) {
                    current = opt.defaultValue;
                }
                ret.put(opt.s, current);
            }
            List<String> stringArgs = cl.getArgList();
            if (args.size() > stringArgs.size()) {
                throw new RuntimeException("Wrong number of arguments at least "+args.size()+" expected, but only " + stringArgs.size() + " found");
            }

            int argIndex = 0;
            int stringArgIndex = 0;
            if (args.size() > 0) {
                while (argIndex < args.size()) {
                    Arg arg = args.get(argIndex);
                    boolean isLastArg = (argIndex == (args.size() - 1));
                    Object current = null;
                    int maxStringIndex = isLastArg ? stringArgs.size() : (stringArgIndex + 1);
                    for (;stringArgIndex < maxStringIndex; stringArgIndex++) {
                        current = arg.process(current, stringArgs.get(stringArgIndex));
                    }
                    ret.put(arg.name, current);
                    argIndex++;
                }
            } else {
                ret.put("ARGS", stringArgs);
            }
            return ret;
        }
    }

    public static CLIBuilder opt(String s, String l, Object defaultValue) {
        return new CLIBuilder().opt(s, l, defaultValue);
    }
 
    public static CLIBuilder opt(String s, String l, Object defaultValue, Parse parse) {
        return new CLIBuilder().opt(s, l, defaultValue, parse);
    }

    public static CLIBuilder opt(String s, String l, Object defaultValue, Parse parse, Assoc assoc) {
        return new CLIBuilder().opt(s, l, defaultValue, parse, assoc);
    }

    public CLIBuilder arg(String name) {
        return new CLIBuilder().arg(name);
    }
 
    public CLIBuilder arg(String name, Assoc assoc) {
        return new CLIBuilder().arg(name, assoc);
    }

    public CLIBuilder arg(String name, Parse parse) {
        return new CLIBuilder().arg(name, parse);
    }

    public CLIBuilder arg(String name, Parse parse, Assoc assoc) {
        return new CLIBuilder().arg(name, parse, assoc);
    }
}
