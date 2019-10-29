/*
    *Java Code  and all content copyright © 2019,
    *Sadhvik Chirunomula |
    *Terms of Use |
    *Privacy Policy |
    *Contact:+919160230298
 */


import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

class pri{
    public String priority(ArrayList a){
        if(a.contains("varchar")||a.contains("text")){
            return "varchar";
        }
        else{
            return "int";
        }
    }

    public ArrayList<String> KeysOfMaptoList(HashMap<String,ArrayList<String>> hm){
        ArrayList<String> a1 = new ArrayList<String>();
        for (String i:hm.keySet()) {
            a1.add(i);
        }
        System.out.println(a1);
        return a1;
    }

    public ArrayList<String> inselectQuery(HashMap<String,ArrayList<String>> hm, HashMap<String,String> hmTableMetaData_Temp){
        ArrayList<String> inselect1 = new ArrayList<String>();
        for (String i: hmTableMetaData_Temp.keySet()){
            ArrayList<String> t =hm.get(i);
            String s = t.get(0);
            if (hmTableMetaData_Temp.get(i).equals(s)){
                inselect1.add(i);
            }
            else{
                String st = "cast("+i+" as "+s+")";
                inselect1.add(st);
            }
        }

        for (String i:hm.keySet()){
            if (!hmTableMetaData_Temp.containsKey(i)){
                String s= "null as "+i;
                inselect1.add(s);
            }
        }

        System.out.println(inselect1);
        return  inselect1;
    }

    public ArrayList<String> organize(ArrayList<String> inselect1,ArrayList<String> inInsert1){
        for(int i=0;i<inInsert1.size();i++){
            String a = inInsert1.get(i);
            for (int j=i;j<inselect1.size();j++){
                String b = inselect1.get(j);
                if (b.contains(a)){
                    Collections.swap(inselect1,i,j);
                }
            }
        }
        return inselect1;
    }
}

class Unification{
    public void UnificationMethod(ArrayList<String> tablesToBeSelected, String output_table_name) throws SQLException {
        final String url = "jdbc:postgresql://localhost:5432/postgres";
        final String user = "postgres";
        final String password = "Temp!23";
        Connection conn = null;
        Statement stmt = null;
        ResultSet ra = null;
        ResultSetMetaData md = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
            stmt = conn.createStatement();
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        pri pr = new pri();

        ArrayList<ResultSet> rslist = new ArrayList<ResultSet>();
        ArrayList<ResultSetMetaData> mdlist = new ArrayList<ResultSetMetaData>();

        for(int i=0;i<tablesToBeSelected.size();i++){
            Statement st = conn.createStatement();
            ResultSet resultSet = st.executeQuery(tablesToBeSelected.get(i));
            ResultSetMetaData metaData = resultSet.getMetaData();
            mdlist.add(metaData);
        }

        //hmTableMetaData_Temp -> Temporary map used in iteration which COntains column name and datatype and is added into list of HashMaps
        HashMap<String,String> hmTableMetaData_Temp = new HashMap<String, String>();

        //hm -> contains output table name and datatype
        HashMap<String,ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();

//      Stores Metadata of all tables
        ArrayList<HashMap<String,String>> inputTableList = new ArrayList<HashMap<String, String>>();

//      md1 for temporary metadata storage
        ResultSetMetaData md1=null;

        for (int j=0;j<mdlist.size();j++) {
            md1 = mdlist.get(j);
            hmTableMetaData_Temp = new HashMap<String, String>();
            for (int i = 1; i<= md1.getColumnCount(); i++) {
                hmTableMetaData_Temp.put(md1.getColumnName(i), md1.getColumnTypeName(i));
                if (!hm.containsKey(md1.getColumnName(i))) {
                    ArrayList<String> t = new ArrayList<String>();
                    t.add(md1.getColumnTypeName(i));
                    hm.put(md1.getColumnName(i), t);
                } else {
                    ArrayList<String> t = hm.get(md1.getColumnName(i));
                    t.add(md1.getColumnTypeName(i));
                    ArrayList<String> t1 = new ArrayList<String>();
                    t1.add(pr.priority(t));
                    hm.put(md1.getColumnName(i),t1);
                }
            }
            inputTableList.add(hmTableMetaData_Temp);
        }
        System.out.println("inputTableList: "+inputTableList);

        System.out.println("HashMap for output Table: "+ hm);

        STGroup unify = new STGroupFile("D:\\Work\\FormatConversion\\Unification\\src\\main\\resources\\StringTemplates\\Unify.stg",'$','$');
        ST create =  unify.getInstanceOf("createTable").add("HashMap",hm).add("table_name",output_table_name);
        String createStmt = create.render();
        System.out.println(createStmt);
        Statement statement = conn.createStatement();
        statement.executeUpdate(createStmt);

        ArrayList<String> inInsert = pr.KeysOfMaptoList(hm);

        //Applying 'casting' and 'as null' in selectListtemp
        ArrayList<ArrayList<String>> selectListtemp = new ArrayList<ArrayList<String>>();
        for (int i=0;i<inputTableList.size();i++) {
            selectListtemp.add(pr.inselectQuery(hm,inputTableList.get(i)));
        }

        //Organizing ArrayListTemp based on insert query parameters
        ArrayList<ArrayList<String>> selectList = new ArrayList<ArrayList<String>>();
        for (int i=0;i<selectListtemp.size();i++){
            selectList.add(pr.organize(selectListtemp.get(i),inInsert));
        }

        HashMap<String,ArrayList<String>> finalMap = new HashMap<String, ArrayList<String>>();
        ArrayList<ArrayList<String>> ar = new ArrayList<ArrayList<String>>();
        for (int i=0;i<selectList.size();i++){
            // mdtemp is created to get table name of respective table
            ResultSetMetaData mdtemp = mdlist.get(i);
            String s = mdtemp.getTableName(1);
            finalMap.put(s,selectList.get(i));
        }

        ST insert = unify.getInstanceOf("printStatements").add("inInsert",inInsert).add("Hm",finalMap).add("table_name",output_table_name);
        String out = insert.render();
        System.out.println();
        System.out.println(out);
        stmt.executeUpdate(out);
    }
}


public class Unify {
    public static void main(String[] args) throws SQLException {
        ArrayList<String> tablesToBeSelected = new ArrayList<String>();
        Collections.addAll(tablesToBeSelected,"select * from t1","select * from t2","select * from t3");
        Unification u = new Unification();
        u.UnificationMethod(tablesToBeSelected,"temp1");
    }
}
