import java.util.*;
import java.util.stream.Collectors;

public class DbFunctions {
    Map<String, Map<String, List<Integer>>> intFields = new HashMap<>();
    Map<String, Map<String, List<Double>>> dblFields = new HashMap<>();
    Map<String, Map<String, List<String>>> strFields = new HashMap<>();

    public String[] splitParams(String arg) {
        if (arg == null) {
            return null;
        }
        return arg.replace("(", "").replace(")", "").trim().split("\\s*,\\s*");
    }

    public String[] splitArgs(String arg) {
        if (arg == null) {
            return null;
        }
        return arg.replace("[", "").replace("]", "").trim().split("\\s*,\\s*");
    }

    public String[] splitVars(String arg) {
        if (arg == null) {
            return null;
        }
        return arg.replace("{", "").replace("}", "").trim().split("\\s*,\\s*");
    }

    public void runCreate(String tabName, String args) {
        try {
            if (!tabName.matches("[a-z0-9_]+")) {
                throw new RuntimeException("table name must only contains alphabetic numeric and _ characters: " + tabName);
            }

            if (strFields.containsKey(tabName) || intFields.containsKey(tabName) || dblFields.containsKey(tabName)) {
                throw new RuntimeException("this table name is already used. choose another one: " + tabName);
            }

            if (args == null) {
                throw new RuntimeException("no columns and its type inside '[name type]' found!");
            }

            Map<String, List<Integer>> columns1 = new HashMap<>();
            Map<String, List<Double>> columns2 = new HashMap<>();
            Map<String, List<String>> columns3 = new HashMap<>();

            String[] result = splitArgs(args);
            for (String res : result) {
                String[] res1 = res.split("[ ]+");

                if (res1.length != 2) {
                    throw new RuntimeException("unmatched parameters for column name and its type: " + res);
                }

                String name = res1[0];
                String type = res1[1];
                switch (type) {
                    case "str":
                        List<String> list = new ArrayList<>();
                        columns3.put(name, list);
                        break;
                    case "int":
                        List<Integer> list1 = new ArrayList<>();
                        columns1.put(name, list1);
                        break;
                    case "dbl":
                        List<Double> list2 = new ArrayList<>();
                        columns2.put(name, list2);
                        break;
                    default:

                        throw new RuntimeException("Unknown Type: " + type);


                }
            }
            strFields.put(tabName, columns3);
            dblFields.put(tabName, columns2);
            intFields.put(tabName, columns1);

        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
        }
    }
    public  boolean runFilter(String tabName, String param, Integer i) {
        try {
            Map<String, List<Integer>> tab1 = intFields.get(tabName);
            Map<String, List<Double>> tab2 = dblFields.get(tabName);
            Map<String, List<String>> tab3 = strFields.get(tabName);

            String filter = param.replace("(", "").replace(")", "").trim();
            String left = filter.split("\\s*[><=]\\s*")[0];
            String right = filter.split("\\s*[><=]\\s*")[1];

            String[] lefts = left.split("\\s*[+\\-]\\s*");
            String[] rights = right.split("\\s*[+\\-]\\s*");
            char op = filter.charAt(left.length());
            List<String> cols = new ArrayList<>();

            double sum = 0;
            for (String l : lefts) {
                int k = left.indexOf(l);
                if (l.matches("([+-]?\\d*\\.*\\d*)")) {
                    if (k == 0) {
                        sum -= Double.parseDouble(l);
                    } else if (left.charAt(k - 1) == '+') {
                        sum -= Double.parseDouble(l);
                    } else {
                        sum += Double.parseDouble(l);
                    }
                } else if(!l.startsWith("'")) {
                    if(!(tab1.containsKey(l) || tab2.containsKey(l) || tab3.containsKey(l))){
                        throw new RuntimeException("the column in filter you entered not found: "+l);
                    }
                    if (k == 0) {
                        cols.add("+" + l);
                    } else if (left.charAt(k - 1) == '+') {
                        cols.add("+" + l);
                        ;
                    } else {
                        cols.add("-" + l);
                        ;
                    }

                }
            }

            for (String r : rights) {
                int k = right.indexOf(r);
                if (r.matches("([+-]?\\d*\\.*\\d*)")) {
                    if (k == 0) {
                        sum += Double.parseDouble(r);
                    } else if (right.charAt(k - 1) == '+') {
                        sum += Double.parseDouble(r);
                    } else {
                        sum -= Double.parseDouble(r);
                    }
                } else if(!r.startsWith("'")){
                    if(!(tab1.containsKey(r) || tab2.containsKey(r) || tab3.containsKey(r))){
                        throw new RuntimeException("the column in filter you entered not found: "+r);
                    }
                    if (k == 0) {
                        cols.add("-" + r);
                    } else if (right.charAt(k - 1) == '+') {
                        cols.add("-" + r);
                        ;
                    } else {
                        cols.add("-" + r);
                        ;
                    }

                }
            }

           List<String> cols1= cols.stream().filter(a->tab1.containsKey(a.substring(1))).collect(Collectors.toList());
            List<String> cols2= cols.stream().filter(a->tab2.containsKey(a.substring(1))).collect(Collectors.toList());
            List<String> cols3= cols.stream().filter(a->tab3.containsKey(a.substring(1))).collect(Collectors.toList());

            if(!cols1.isEmpty() && cols2.isEmpty() && cols3.isEmpty()) {
                List<Integer> list = cols.stream().map(a -> tab1.get(a.substring(1))).filter(Objects::nonNull).map(b -> b.get(i)).collect(Collectors.toList());
                List<Integer> list1 = list.stream().filter(a -> (cols.get(list.indexOf(a)).charAt(0) == '+')).collect(Collectors.toList());
                List<Integer> list2 = list.stream().filter(a -> (cols.get(list.indexOf(a)).charAt(0) == '-')).collect(Collectors.toList());

                int eq = 0;
                if (!list1.isEmpty()) {
                    eq += list1.stream().reduce((a, b) -> (a + b)).orElse(null);
                }

                if (!list2.isEmpty()) {
                    eq -= list2.stream().reduce((a, b) -> (a + b)).orElse(null);
                }

                if (op == '>') {
                    return eq > sum;
                } else if (op == '<') {
                    return eq < sum;
                } else if (op == '=') {
                    return eq == sum;
                }
            }else if(cols1.isEmpty() && !cols2.isEmpty() && cols3.isEmpty()){
                List<Double> list = cols.stream().map(a -> tab2.get(a.substring(1))).filter(Objects::nonNull).map(b -> b.get(i)).collect(Collectors.toList());
                List<Double> list1 = list.stream().filter(a -> (cols.get(list.indexOf(a)).charAt(0) == '+')).collect(Collectors.toList());
                List<Double> list2 = list.stream().filter(a -> (cols.get(list.indexOf(a)).charAt(0) == '-')).collect(Collectors.toList());

                int eq = 0;
                if (!list1.isEmpty()) {
                    eq += list1.stream().reduce((a, b) -> (a + b)).orElse(null);
                }

                if (!list2.isEmpty()) {
                    eq -= list2.stream().reduce((a, b) -> (a + b)).orElse(null);
                }

                if (op == '>') {
                    return eq > sum;
                } else if (op == '<') {
                    return eq < sum;
                } else if (op == '=') {
                    return eq == sum;
                }
            }else if(cols1.isEmpty() && cols2.isEmpty() && !cols3.isEmpty()) {
                String str = cols.stream().map(a -> tab3.get(a.substring(1))).filter(Objects::nonNull).map(b -> b.get(i)).findFirst().orElse(null);
                return str.equals(right);
            }else{
                throw new RuntimeException("invalid column type in filter: "+param);
            }
        }catch (RuntimeException e){
            System.err.println(e.getMessage());
        }

        return true;
    }

    public void runGet(String tabName, String param) {
        try {
            if (!(strFields.containsKey(tabName) || intFields.containsKey(tabName) || dblFields.containsKey(tabName))) {
                throw new RuntimeException("table you provided, not found " + tabName);
            }
            Map<String, List<Integer>> tab1 = intFields.get(tabName);
            Map<String, List<Double>> tab2 = dblFields.get(tabName);
            Map<String, List<String>> tab3 = strFields.get(tabName);

            Set<String> colNames1 = tab1.keySet();
            Set<String> colNames2 = tab2.keySet();
            Set<String> colNames3 = tab3.keySet();

            for (String name : colNames1) {
                System.out.printf("%25s", name + "|");

            }

            for (String name : colNames2) {
                System.out.printf("%25s", name + "|");

            }

            for (String name : colNames3) {
                System.out.printf("%25s", name + "|");

            }
            System.out.println();
            Collection<List<Integer>> vals1 = tab1.values();

            int l;
            if (vals1.stream().findFirst().orElse(null) != null) {
                l = vals1.stream().findFirst().orElse(null).size();
            } else {
                l = 0;
            }
            Collection<List<Double>> vals2 = tab2.values();
            Collection<List<String>> vals3 = tab3.values();


            for (int i = 0; i < l; i++) {
                for (List<Integer> val : vals1) {
                    if (param == null || runFilter(tabName, param, i)) {
                        System.out.printf("%25s", val.get(i) + "|");
                    }

                }
                for (List<Double> val : vals2) {

                    if (param == null || runFilter(tabName, param, i)) {
                        System.out.printf("%25s", val.get(i) + "|");
                    }
                }

                for (List<String> val : vals3) {

                    if (param == null || runFilter(tabName, param, i)) {
                        System.out.printf("%25s", val.get(i) + "|");
                    }
                }
                if (param == null || runFilter(tabName, param, i)) {
                    System.out.println();
                }

            }
        }
        catch (RuntimeException e){
            System.err.println(e.getMessage());
        }

    }

    public void runAdd(String tabName, String vars) {
        try {
            if (!(strFields.containsKey(tabName) || intFields.containsKey(tabName) || dblFields.containsKey(tabName))) {
                throw new RuntimeException("table you provided, not found " + tabName);
            }
            Map<String, List<Integer>> tab1 = intFields.get(tabName);
            Map<String, List<Double>> tab2 = dblFields.get(tabName);
            Map<String, List<String>> tab3 = strFields.get(tabName);
            List<String> cols = new ArrayList<>();
            if (vars != null) {
                String[] result = splitVars(vars);
                for (String res : result) {
                    String name = res.split("\\s*=\\s*")[0];
                    String val = res.split("\\s*=\\s*")[1];

                    if (tab1.containsKey(name)) {
                        if(!val.matches("[+-]?\\d+")) {
                            throw new RuntimeException("wrong type 'int' for:  " + val);
                        }
                        tab1.get(name).add(Integer.valueOf(val));
                        cols.add(name);
                    } else if (tab2.containsKey(name)) {
                        if(!val.matches("([+-]?\\d*\\.+\\d*)")) {
                            throw new RuntimeException("wrong type 'dbl' for:  "+val);
                        }
                            tab2.get(name).add(Double.valueOf(val));
                        cols.add(name);
                    } else if (tab3.containsKey(name)) {
                        if(!(val.startsWith("'") && val.endsWith("'")) ){
                            throw new RuntimeException("wrong type 'str' for:  "+val);
                        }
                        tab3.get(name).add(val);
                        cols.add(name);
                    } else{
                        throw new RuntimeException("not such column name found: "+name);
                    }
                }
            }

            tab1.keySet().stream().filter(a -> !cols.contains(a))
                    .forEach(b -> tab1.get(b).add(0));

            tab2.keySet().stream().filter(a -> !cols.contains(a))
                    .forEach(b -> tab2.get(b).add(0.0));

            tab3.keySet().stream().filter(a -> !cols.contains(a))
                    .forEach(b -> tab3.get(b).add("''"));


            tab1.keySet().forEach(a-> System.out.printf("%25s", a + "|"));
            tab2.keySet().forEach(a-> System.out.printf("%25s", a + "|"));
            tab3.keySet().forEach(a-> System.out.printf("%25s", a + "|"));
            System.out.println();
            tab1.values().stream().map(b->b.get(b.size()-1)).forEach(a-> System.out.printf("%25s", a + "|"));
            tab2.values().stream().map(b->b.get(b.size()-1)).forEach(a-> System.out.printf("%25s", a + "|"));
            tab3.values().stream().map(b->b.get(b.size()-1)).forEach(a-> System.out.printf("%25s", a + "|"));
            System.out.println();


        }
        catch (RuntimeException e){
            System.err.println(e.getMessage());
        }

    }

    public void runDrop(String tabName) {
        try {
            if (!(strFields.containsKey(tabName) || intFields.containsKey(tabName) || dblFields.containsKey(tabName))) {
                throw new RuntimeException("table you provided, not found " + tabName);
            }
            intFields.remove(tabName);
            dblFields.remove(tabName);
            strFields.remove(tabName);
        }catch(RuntimeException e){
            System.err.println(e.getMessage());
        }

    }

    public void runSet(String tabName, String param, String var) {
        try {
            if (!(strFields.containsKey(tabName) || intFields.containsKey(tabName) || dblFields.containsKey(tabName))) {
                throw new RuntimeException("table you provided, not found " + tabName);
            }
            String[] varResult = splitVars(var);
            String[] paramResult = splitParams(param);

            Map<String, List<Integer>> tab1 = intFields.get(tabName);
            Map<String, List<Double>> tab2 = dblFields.get(tabName);
            Map<String, List<String>> tab3 = strFields.get(tabName);

            int count = 0;
            if (paramResult != null) {

                        int l;
                        Collection<List<Integer>> vals1 = tab1.values();
                        if (vals1.stream().findFirst().orElse(null) != null) {
                            l = vals1.stream().findFirst().orElse(null).size();
                        } else {
                            l = 0;
                        }
                        Collection<List<Double>> vals2 = tab2.values();
                        Collection<List<String>> vals3 = tab3.values();

                        for(int i=0;i<l;i++){
                            if ( runFilter(tabName, param, i)) {
                                count++;
                                for (String v : varResult) {
                                    String name = v.split("\\s*=\\s*")[0];
                                    String value = v.split("\\s*=\\s*")[1];
                                for (List<Integer> val : vals1) {
                                    if(tab1.containsKey(name)) {
                                        if(!value.matches("[+-]?\\d+")) {
                                            throw new RuntimeException("wrong type 'int' for:  " + value);
                                        }
                                        val.set(i, Integer.valueOf(value));
                                    }
                                }

                                for (List<Double> val : vals2) {
                                    if(tab2.containsKey(name)) {
                                        if(!value.matches("([+-]?\\d*\\.+\\d*)")) {
                                            throw new RuntimeException("wrong type 'dbl' for:  "+value);
                                        }
                                        val.set(i, Double.valueOf(value));
                                    }
                                }

                                for (List<String> val : vals3) {
                                    if(tab3.containsKey(name)) {
                                        if(!(value.startsWith("'") && value.endsWith("'")) ){
                                            throw new RuntimeException("wrong type 'str' for:  "+value);
                                        }
                                    val.set(i,value);
                                    }
                                }

                                    if(!(tab1.containsKey(name) || tab2.containsKey(name) || tab3.containsKey(name))){
                                        throw new RuntimeException("not such column found: "+name);
                                    }

                                    }
                        }

                }
            } else {
                for (String v : varResult) {
                    String name = v.split("\\s*=\\s*")[0];
                    String val = v.split("\\s*=\\s*")[1];
                    if (tab1.containsKey(name)) {
                        List<Integer> cont = tab1.get(name);
                        for (int i = 0; i < cont.size(); i++) {
                            if(!val.matches("[+-]?\\d+")) {
                                throw new RuntimeException("wrong type 'int' for:  " + val);
                            }
                            cont.set(i, Integer.valueOf(val));
                            count++;
                        }
                    } else if (tab2.containsKey(name)) {
                        List<Double> cont = tab2.get(name);
                        for (int i = 0; i < cont.size(); i++) {
                            if(!val.matches("([+-]?\\d*\\.+\\d*)")) {
                                throw new RuntimeException("wrong type 'dbl' for:  "+val);
                            }
                            cont.set(i, Double.valueOf(val));
                            count++;
                        }
                    } else if (tab3.containsKey(name)) {
                        List<String> cont = tab3.get(name);
                        for (int i = 0; i < cont.size(); i++) {
                            if(!(val.startsWith("'") && val.endsWith("'")) ){
                                throw new RuntimeException("wrong type 'str' for:  "+val);
                            }
                            cont.set(i, val);
                            count++;
                        }
                    }else{
                        throw new RuntimeException("not such column name found: "+name);
                    }
                }

            }

            System.out.println(count + " row(s) affected.");
            System.out.println();
        } catch (RuntimeException e){
            System.err.println(e.getMessage());
        }
    }

    public void runDelete(String tabName, String param) {
        try {
            if (!(strFields.containsKey(tabName) || intFields.containsKey(tabName) || dblFields.containsKey(tabName))) {
                throw new RuntimeException("table you provided, not found " + tabName);
            }

            Map<String, List<Integer>> tab1 = intFields.get(tabName);
            Map<String, List<Double>> tab2 = dblFields.get(tabName);
            Map<String, List<String>> tab3 = strFields.get(tabName);
            int count = 0;
            if (param != null) {
                int l;
                Collection<List<Integer>> vals1 = tab1.values();
                if (vals1.stream().findFirst().orElse(null) != null) {
                    l = vals1.stream().findFirst().orElse(null).size();
                } else {
                    l = 0;
                }
                Collection<List<Double>> vals2 = tab2.values();
                Collection<List<String>> vals3 = tab3.values();

              for(int i=0;i<l;i++){
                  if ( runFilter(tabName, param, i)) {
                     count++;
                      for (List<Integer> val : vals1) {
                          val.remove(i);
                      }


                      for (List<Double> val : vals2) {

                          val.remove(i);

                      }

                      for (List<String> val : vals3) {
                          val.remove(i);

                      }
                  }
              }

            } else {
                count=0;
                count += (int) tab1.values().stream().collect(Collectors.toList()).get(0).size();

                tab1.values().forEach(List::clear);
                tab2.values().forEach(List::clear);
                tab3.values().forEach(List::clear);

            }
            System.out.println(count + " row(s) deleted");
        }catch (RuntimeException e){
            System.err.println(e.getMessage());
        }
    }
}



