import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class View {
    Scanner scanner = new Scanner(System.in);
    DbFunctions functions=new DbFunctions();
    public String startDb(){
        try {
            String input;
            String regex = "[ ]+";

            //Compiling the regular expression
            Pattern pattern = Pattern.compile(regex);
            input = scanner.nextLine();
            input = input.toLowerCase();
            if (input.equals("exit")) {
                return "exit";
            }
            //Retrieving the matcher object
            Matcher matcher = pattern.matcher(input);
            String result = matcher.replaceAll(" ");
            result = result.replace("(", " (").replace("[", " [").replace("{", " {");
            int i = 0;
            int j = 0;
            int k = 0;

            if (result.indexOf("(") > 0) {
                i = result.indexOf("(");
            }
            if (result.indexOf("[") > 0) {
                j = result.indexOf("[");
            }

            if (result.indexOf("{") > 0) {
                k = result.indexOf("{");
            }

            String result1 = null;
            if (Math.min(i, Math.min(j, k)) > 0) {
                result1 = result.substring(0, Math.min(i, Math.min(j, k)));
            } else {
                result1 = result;
            }

            String[] com_table = result1.split(" ");
            if (com_table.length < 2) {
              throw new RuntimeException("invalid sql command syntax: "+result1);
            }
            String command = result1.split(" ")[0];

            String table = result1.split(" ")[1];

            String remain = result.replace(command, "").replace(table, " ");
            String a = null;
            Matcher matcher1 = Pattern.compile("\\((.*?)\\)").matcher(remain);
            if (matcher1.find()) {
                a = matcher1.group(0);
            }
            String b = null;
            Matcher matcher2 = Pattern.compile("\\[(.*?)]").matcher(remain);
            if (matcher2.find()) {
                b = matcher2.group(0);
            }
            String c = null;
            Matcher matcher3 = Pattern.compile("\\{(.*?)}").matcher(remain);
            if (matcher3.find()) {
                c = matcher3.group(0);
            }

            selectCommand(table, command, a, b, c);
        } catch (RuntimeException e){
            System.err.println(e.getMessage());
        }
        return "continue";
    }

    public void selectCommand(String tab,String comm,String param,String arg,String var){
        try{
        switch(comm) {
            case "create":
                if(arg==null){
                    throw new RuntimeException("'create' needs args between [] ");
                }
                functions.runCreate(tab, arg);
                break;
            case "get":
                functions.runGet(tab, param);
                break;
            case "add":
                functions.runAdd(tab, var);
                break;
            case "drop":
                functions.runDrop(tab);
                break;
            case "set":
                if(var==null){
                    throw new RuntimeException("'create' needs args between {} ");
                }
                functions.runSet(tab, param, var);
                break;
            case "del":
                functions.runDelete(tab, param);
                break;
        }
        } catch (RuntimeException e){
            System.err.println(e.getMessage());
        }
    }


}
