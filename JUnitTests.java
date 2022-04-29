import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import java.io.FileReader;
import java.io.IOException;

import org.apache.ibatis.jdbc.ScriptRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.io.FileUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;

/** 
 * Die Testklasse beinhaltet die, Tests, eine setUp Methode und createDb Methode
 * Die Annotation "TestMethodOrder(OrderAnnotation.class)" wird benötigt, um die Tests in gegebener Reihenfolge ausführen zu können
 * Die Annotation "TestInstance(Lifecycle.PER_CLASS)" wird benötigt, um die "BeforeAll"-Annotation in der setUP-Methoden nutzen zu können
 * 
 * Da nicht alle Datenbanken die Möglichkeit bieten, alle Tests durchzuführen, wird in jedem Test vorher evaluiert um welche Datenbank es sich derzeitig handelt.
 * Bietet eine Datenbank nicht die Möglichkeit der Funktion, auch nicht eingeschränkt, wird durch "assertTrue(false)", ein fehlerhafter Test simuliert.
 * Hierdurch soll visuell gezeigt werden, welche der vorgestellten Funktionen bei welcher Datenbank funktionieren.
 * 
 * Es werden zu Beginn in der Testklasse ein leeres Connection-Objekt erstellt, welches in der setUp-Methode für die jeweilige Datenbank mit entsprechenden Parametern initialisiert wird.
 * Darüber hinaus legt der String "currentDatabase" die zu initialisierende Datenbank fest. Es werden also die tests auf der Datenbank ausgeführt, welche in diesem String angegeben ist.
 * Zur Auswahl stehen: Derby, h2sql, hsqldb
 * queryList beinhaltet die in den jeweiligen Methoden (Beispiel: grouping_sets() um Grouping Sets zu erstellen) erstellten Queries. Diese werden an die write-Methoden 
 * (writeToList oder writeTo String) übergeben und darin ausgeführt und returnt.
 */
@TestMethodOrder(OrderAnnotation.class)
@TestInstance(Lifecycle.PER_CLASS)
public class JUnitTests
{
    String currentDatabase = "";
    Connection conn = null;
    ArrayList<String> queryList = new ArrayList<String>();
    int counter = 0;

    /**Lädt die beiden SQL Dateien und fügt deren Daten in die im Connection-Objekt definierte Datenbank ein*/
    void createDb()
    {
        try
        {
            ScriptRunner sr = new ScriptRunner(conn);
            Reader reader = new BufferedReader(new FileReader("C:\\Yannick\\Bachelor\\Java Projekt\\DbData\\Sales.sql"));
            sr.runScript(reader);
            reader = new BufferedReader(new FileReader("C:\\Yannick\\Bachelor\\Java Projekt\\DbData\\Mitarbeiter.sql"));
            sr.runScript(reader);
        }
        catch(Exception e){
            System.out.println(e);
        }
    }

    /**
     * Diese Methode wird einmalig vor den Tests ausgeführt und stellt die Datenbank-Verbindung her. Diese Verbindung wird im Connection-Objekt (conn) gespeichert.
     * Über switch case wird anhand der Defintion des "currentDatabase"-Strings entschieden, zu welcher Datenbank die Verbindung hergestellt werden soll
    */
    @BeforeEach
    void setUp()
    {
        currentDatabase = "hsql"; //H2SQL: "h2"; HSQLDB: "hsql"; Apache Derby: "derby"
        String dbName = "";
        String protocol = "";
        
        switch(currentDatabase){
            // Verbindung zu hsqldb
            case "hsql":
                dbName = "HSQLDB";
                protocol = "jdbc:hsqldb:mem";
                break;
            // Verbindung zu Derby Datenbank. Besonderheit hier ist das ";create=True" Attribut, welches benötigt wird um eine neue Datenbank zu erstellen, sollte Derby auf der URL keine bereits existierende Datenbank finden
            case "derby":
                dbName = "Apache Derby";
                protocol = "jdbc:derby:memory:;create=True";
                break;
            case "h2":
            // Verbindung zu h2sql
                dbName = "H2";
                protocol = "jdbc:h2:mem:";
                break;
            default:
                System.out.println("Kein gültiger DB-Name!");
                break;
        }
        System.out.println(dbName + " starting");
        try
        {
            conn = DriverManager.getConnection(protocol);
            createDb(); //ruft die createDB-Methode auf
        }
        catch(Exception e){
            System.out.println(e);
        }
        finally {
            if (conn == null){
                System.out.println("No Connection");
            }else{
                System.out.println("Connected to database " + dbName + "\n---------------------------------------\n\n");
            }
        }
    }

    /**
     * Methode um Liste in String zu schreiben und somit für Ausgabe nicht jedes Mal iterieren zu müssen
     * @param printList
     * @return Liste als String
     */
    public String printList(ArrayList<String> printList){
        String str = "";
        
        for(int i = 0; i < printList.size();i++){
            str = str + printList.get(i) +"\n";
        }
        return str;
    }


    /**
     * Nimmt die Queries aus der queryList entgegen und erstellt mit dem conn-Objekt ein Statement.
     * Statement wird ausgeführt und schreibt Ergebnis in result, welches ein String-Objekt ist.
     * Nach jeder Zeile wird ein Absatz eingefügt, um das Ergebnis visuell in Tabellenform zu sehen.
     * Entfernt ebenfalls alle "null" aus dem Ergebnis
     * @param queryList Liste der Queries der aufrufenden Methode
     * @return formatiertes String-Objekt mit Ergebnis
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public String writeToString(ArrayList<String> queryList)throws Exception{
        String result = "";

        try 
        {
            Statement s = conn.createStatement();
            for (int x = 0; x < queryList.size();x++)
            {
                ResultSet rs = s.executeQuery(queryList.get(x));
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                while (rs.next()) {
                    String str = "";       
                    for(int i = 1 ; i <= columnsNumber; i++){
                        str = str + rs.getString(i);
                    }
                    str = str + "\n"; //Füge Absatz ein nach Zeilen-Ende
                    result = result + str;
                    str = "";          
                }
            }
            queryList.clear();
        }catch(Exception e){
            e.printStackTrace();
        }
        result = result.trim().replaceAll("\r", ""); //Entferne Steuerzeichen, welche im equals-Vergleich Probleme verursachen können
        result = result.replace("null", "");
        return result;
    }

    /**
     * Nimmt die Queries aus der queryList entgegen und erstellt mit dem conn-Objekt ein Statement.
     * Statement wird ausgeführt und schreibt Ergebnis in result, welches eine ArrayList mit Stringobjekten ist.
     * Jedes Stringobjekt in der Liste steht hierbei für eine Zeile des Results.
     * Entfernt ebenfalls alle "null" aus dem Ergebnis
     * @param queryList Liste der Queries der aufrufenden Methode
     * @return ArrayList mit String mit Ergebnis
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public ArrayList<String> writeToList(ArrayList<String> queryList) throws Exception {
        ArrayList<String> result = new ArrayList<String>();

        try 
        {
            Statement s = conn.createStatement();
            for (int x = 0; x < queryList.size();x++)
            {
                ResultSet rs = s.executeQuery(queryList.get(x));
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                while (rs.next()) {
                    String str = "";       
                    for(int i = 1 ; i <= columnsNumber; i++){
                        str = str + rs.getString(i);
                    }
                    str = str.replace("null", "");        
                    result.add(str);
                    str = "";          
                }
            }
            queryList.clear();
        }catch(Exception e){
            e.printStackTrace();
            throw e;
        }
        return result;
    }

    /**
     * Setzt 2 ArrayLists in 2 HashSets um, um Vergleichbarkeit bei unterschiedlicher Reihenfolge zu ermöglichen
     * @param list1 
     * @param list2
     * @return
     * @throws Exception
     */
    public boolean compareLists(ArrayList<String> list1, ArrayList<String> list2)throws Exception{
        return new HashSet<>(list1).equals(new HashSet<>(list2));
    }

    /**
     * Simuliert das Ergebnis welches bei Anwendung des SQL-Befehls Grouping Sets entstehen würde.
     * Wird genutzt um Grouping Sets Klausel zu vergleichen.
     * Fügt die Queries zu queryList hinzu und ruft anschließend die writeToList-Methode auf
     * @return Aufruf der Methode writeToList
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public ArrayList<String> fake_grouping_sets() throws Exception{

        String queryKontinent = "SELECT Kontinent, sum(menge_Verkaeufe) FROM sales.Verkaeufe GROUP BY Kontinent";

        String queryLand = "SELECT Land, sum(menge_Verkaeufe) FROM sales.Verkaeufe GROUP BY Land";

        String queryStadt = "SELECT Stadt, menge_Verkaeufe FROM sales.Verkaeufe";

        queryList.add(queryKontinent);
        queryList.add(queryLand);
        queryList.add(queryStadt);

        /*Jede Query stellt ein Grouping Set dar. Vergleiche hierzu die GROUP BY-Argumente in der Query der grouping_sets-Methode 
          Durch Kombination der 3 Queries kann Ergebnis von Grouping Sets nachgeahmt werden ohne Grouping Sets zu nutzen
        */
        
        return writeToList(queryList);
    }

    /**
     * Erstellt Query um Grouping Sets Befehl auszuführen
     * Fügt die Query zu queryList hinzu und ruft anschließend die writeToList-Methode auf
     * @return Aufruf der Methode writeToList
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public ArrayList<String> grouping_sets() throws Exception{
        String groupingSetsQuery = "SELECT Kontinent, Land, Stadt, sum(menge_Verkaeufe) FROM sales.Verkaeufe " +
        "GROUP BY GROUPING SETS(Kontinent, Land, Stadt)";

        queryList.add(groupingSetsQuery);
        return writeToList(queryList);
    }

    /**
     * Simuliert das Ergebnis welches bei Anwendung des SQL-Befehls Rollup entstehen würde.
     * Wird genutzt um Rollup Befehl zu vergleichen.
     * Fügt die Queries zu queryList hinzu und ruft anschließend die writeToList-Methode auf
     * @return Aufruf der Methode writeToList
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public ArrayList<String> fake_rollup_query() throws Exception{
        String querySum = "SELECT SUM(menge_Verkaeufe) FROM sales.Verkaeufe"; //Berechnet Gesamtverkäufe

        String queryAll = "SELECT Kontinent, Land, Stadt, sum(menge_Verkaeufe) FROM sales.Verkaeufe " +
        "GROUP BY Kontinent, Land, Stadt";   //Berechnet Gesamtverkäufe nach Stadt     

        String queryLand = "SELECT Kontinent, Land, sum(menge_Verkaeufe) FROM sales.Verkaeufe " +
        "GROUP BY Kontinent, Land"; //Berechnet Gesamtverkäufe nach Land

        String queryKontinent = "SELECT Kontinent, sum(menge_Verkaeufe) FROM sales.Verkaeufe " +
        "GROUP BY Kontinent"; //Berechnet Gesamtverkäufe nach Kontinent

        queryList.add(querySum);
        queryList.add(queryAll);
        queryList.add(queryLand);
        queryList.add(queryKontinent);

        /*
        Jede Query stellt einen Zwischenabschnitt dar, welcher in der Rollup Query automatisch erstellt wird.
        Zusammenfügen dieser simuliert Rollup-Query Ergebnis
        */

        return writeToList(queryList);
    }

    /**
     * Erstellt Query um Rollup Befehl auszuführen
     * Fügt die Query zu queryList hinzu und ruft anschließend die writeToList-Methode auf
     * @return Aufruf der Methode writeToList
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public ArrayList<String> rollup() throws Exception{
        String rollupQuery = "SELECT Kontinent, Land, Stadt, sum(menge_Verkaeufe) FROM sales.Verkaeufe " +
        "GROUP BY ROLLUP(Kontinent, Land, Stadt)";

        queryList.add(rollupQuery);

        return writeToList(queryList);
    }

    /**
     * Erstellt Query um ROW_Number Befehl mit Window-Funktion und dazugehöriger Partitionierung nach Land auszuführen
     * Fügt die Query zu queryList hinzu und ruft anschließend die writeToString-Methode auf
     * @return Aufruf der Methode writeToString
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public String row_number_with_window_function_and_partition()throws Exception{
        String rowNumberQuery = "select *, " +
        "row_number() over(partition by Kontinent order by menge_Verkaeufe) as rn " +
        "from sales.Verkaeufe " +
        "order by kontinent, rn";

        queryList.add(rowNumberQuery);

        return writeToString(queryList);
    }

    /**
     * Erstellt Query um ROW_Number Befehl mit Window-Funktion ohne dazugehörige Partitionierung nach Land auszuführen
     * Da keine Partitionierung nach PARTITION BY stattfindet, wird dieser anhand mehrerer Reultsets simuliert. Die Resultsets werden mit UNION verbunden
     * Fügt die Query zu queryList hinzu und ruft anschließend die writeToString-Methode auf
     * @return Aufruf der Methode writeToString
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public String row_number_with_window_function_without_partition()throws Exception{
        String rowNumberQuery = "SELECT * FROM ( " +
        "SELECT Kontinent, Land, Stadt, menge_Verkaeufe, ROW_NUMBER() OVER() as Platzierung FROM ( " + //Erstellt Asien ResultSet
            "SELECT Kontinent, Land, Stadt, menge_Verkaeufe " +
            "FROM sales.Verkaeufe " +
            "WHERE KONTINENT = 'Asien' " +
            "ORDER BY menge_Verkaeufe) konti " +
        "GROUP BY konti.kontinent, konti.Land, konti.Stadt, konti.menge_Verkaeufe " +
        "UNION " +
        "SELECT Kontinent, Land, Stadt, menge_Verkaeufe, ROW_NUMBER() OVER() as Platzierung FROM ( " + //Erstellt Asien ResultSet
            "SELECT Kontinent, Land, Stadt, menge_Verkaeufe " + 
            "FROM sales.Verkaeufe " +
            "WHERE KONTINENT = 'Europa' " +
            "ORDER BY menge_Verkaeufe) konti " +
          "GROUP BY konti.kontinent, konti.Land, konti.Stadt, konti.menge_Verkaeufe " +
        "UNION " +
          "SELECT Kontinent, Land, Stadt, menge_Verkaeufe, ROW_NUMBER() OVER() as Platzierung FROM ( " + //Erstellt Asien ResultSet
            "SELECT Kontinent, Land, Stadt, menge_Verkaeufe " +
            "FROM sales.Verkaeufe " +
            "WHERE KONTINENT = 'Nord Amerika' " +
            "ORDER BY menge_Verkaeufe) konti " +
          "GROUP BY konti.kontinent, konti.Land, konti.Stadt, konti.menge_Verkaeufe) alles " +
      "ORDER BY alles.Kontinent, Platzierung";

        queryList.add(rowNumberQuery);

        return writeToString(queryList);
    }

    /**
     * Erstellt ein XML File mit den Funktionen welche h2 dafür bietet.
     * Es gibt keine direkt Export-FUnktion für XML, so muss mit GROUP_CONCAT das Ergebnis des SELECT-Befehls aus der Verkaeufe-Tabelle in eine Spalte geschrieben werden
     * um dieses anschließend mit den XML-Funktionen in ein XML-Format in einen View zu schreiben.
     * Abschließend wird der Inhalt des Views in eine Datei mittels "FILE_WRITE" geschrieben
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public void xml_h2()throws Exception{
        String filepath = "C:/Test/sales.xml";

        String createXMLQuery = "CREATE VIEW xml as SELECT "+
            "XMLSTARTDOC() ||"+
            "XMLNODE('Verkaufstabelle', XMLATTR('version','1.0'),"+ //Erstellt Hauptnode "Verkaufstabelle"
                "XMLNODE('Verkaeufe', NULL,"+ //Erstellt Node in Hauptnode "Verkaeufe"
                    "GROUP_CONCAT("+
                        "XMLNODE('Verkauf', NULL,"+ //Erstellt Node in Verkaeufe-Node "Verkauf"
                            "XMLNODE('Kontinent', NULL, sales.Verkaeufe.kontinent) ||"+ //Erstellt die jeweiligen Attribute für die Verkauf Node
                            "XMLNODE('Land', NULL, sales.Verkaeufe.land) ||"+
                            "XMLNODE('Stadt', NULL, sales.Verkaeufe.stadt) ||"+
                            "XMLNODE('Menge', NULL, sales.Verkaeufe.menge_Verkaeufe)"+
                        ")"+
                    "ORDER BY sales.Verkaeufe.kontinent DESC SEPARATOR '')"+
                ")"+
            ") CONTENT "+
        "FROM sales.Verkaeufe";

        String saveXMLQuery = "SELECT FILE_WRITE(content, '" +filepath+"') FROM xml";

        try{
            Statement s = conn.createStatement();
            s.execute(createXMLQuery);
            s.execute(saveXMLQuery);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Erstellt den table xml_data, welcher Daten in xml-Format speichert. Views bieten nicht die Möglichkeit.
     * Mithilfe der XMLPARSE-Funktion von Derby wird aus der Verkaeufe-Tabelle jede Zeile genommen und in XML Format geschrieben.
     * Jede Zeile wird als einzelne Zeile in xml_data geschrieben
     * Anschließend wird xml_data mit dem XMLSERIALIZE_Befehl serialisiert und dem SELECT zur Verfügung gestellt
     * Diese SELECT Query wird an queryList übergeben und anschließend weiter an writeToString
     * Derby bietet keine Möglichkeit des XML-Exports in eine Datei (https://db.apache.org/derby/docs/10.7/tools/ctoolsimport27052.html, Absatz "Data Types")
     * @return Aufruf der Methode writeToString
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public String xml_derby()throws Exception{
        String createQuery = "CREATE TABLE xml_data(xml_col XML)";

        String insertQuery = "INSERT INTO xml_Data " +
        "SELECT( " +
          "XMLPARSE(DOCUMENT( " +
          "'<Verkauf>' || " +     //Erstellt Node "Verkauf"
             "'<Kontinent>'|| sales.Verkaeufe.kontinent ||'</Kontinent>' || " +    //Erstellt die jeweiligen Attribute für die Verkauf Node              
             "'<Land>'|| sales.Verkaeufe.Land ||'</Land>'|| " +   
             "'<Stadt>'|| sales.Verkaeufe.Stadt ||'</Stadt>'|| " +
             "'<Menge>'|| TRIM(CHAR(sales.Verkaeufe.menge_Verkaeufe)) ||'</Menge>'|| " +             
           "'</Verkauf>' " + 
         ")PRESERVE WHITESPACE)) " +          
        "FROM sales.Verkaeufe";

        String selectQuery = "SELECT XMLSERIALIZE(xml_col AS CLOB) FROM xml_data";

        try{
            Statement s = conn.createStatement();
            s.execute(createQuery);
            s.execute(insertQuery);
        }catch(Exception e){
            e.printStackTrace();
        }
        queryList.add(selectQuery);
        return writeToString(queryList);
    }

    /**
     * Erstellt ein JSON File mit den Funktionen welche h2 dafür bietet.
     * Es gibt keine direkt Export-FUnktion für json. Es wird ein View erstellt welcher die alle Spalten der Verkaeufe Tabelle beinhaltet. 
     * Die Werte werden als varchar oder int gecastet und Zeile für Zeile über den JSON_OBJECT-Befehl in JSON umgewandelt
     * Die JSON-Objekte werden anschließend in einen JSON-Array mit dem JSON_ARRAY Befehl geschrieben.
     * Abschließend wird der Inhalt des Views in eine Datei mittels "FILE_WRITE" geschrieben
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public String JSON_h2(boolean create)throws Exception{
        String filepath = "C:/Test/sales_h2.json";
        Statement s = null;
        String antwort = "";

        String viewObjQuery = "CREATE VIEW jsonView as SELECT " +
        "JSON_OBJECT( \n" + //Erstellt JSON-Objekt für jeweilige Zeile
          "'Kontinent' : CAST(sales.Verkaeufe.Kontinent AS VARCHAR(30)), " +
          "'Land' : CAST(sales.Verkaeufe.Land AS VARCHAR(30)), " +
          "'Stadt' : CAST(sales.Verkaeufe.Stadt AS VARCHAR(30)), " +
          "'menge_Verkaeufe' : CAST(sales.Verkaeufe.menge_Verkaeufe AS INT)) " +
          "CONTENT " +
          "FROM sales.Verkaeufe;";

        String viewArrayQuery = "create view arrayView as " +
        "SELECT JSON_ARRAY((SELECT content FROM jsonView) FORMAT JSON) Content;"; //Erstellt mit JSON-Array mit entsprechenden Objekten als Inhalt

        String fileQuery = "SELECT FILE_WRITE(content, '"+ filepath +"') FROM arrayView";

        String getJson = "SELECT * FROM arrayView";   

        try {
            s = conn.createStatement();
        
            s.execute(viewObjQuery);
            s.execute(viewArrayQuery);
            counter ++;
        }catch(Exception e){
            e.printStackTrace();
        }

        if (create)
        {
            try{
                s.execute(fileQuery);
                antwort = "JSON-Datei erfolgreich erstellt";
            }catch(Exception e){
                e.printStackTrace();
                antwort = "JSON-Datei erfolglos erstellt";
            }
        }
        else
        {
            queryList.add(getJson);
            antwort = writeToString(queryList);
        }

        return antwort;
    }

    /**
     * Derby bietet eine JSON Funktion über den Import von simpleJson
     * Der Import konnte aufgrund des Apostrophes in der letzten Zeile des String "jsonFunction" nicht ausgeführt werden.
     * Derby hat das Apostrophe immer als Backslash,Apostrophe (\') interpretiert und daraufhin eine SQLSyntaxErrorException geworfen
     * Versuche um Problem zu lösen:
     *  - Einfügen des Apostrophes über PreparedStatement schlug fehl
     *  - Doppelte Apostrophe schlug fehl
     *  - Funktion "escapeSQL" der apache StringEscapeUtils schlug fehl (laut Dokumentation fügt diese ebenfalls nur doppelte Apostrophe ein)
     * @throws Exception
     */
    public String JSON_derby(boolean create)throws Exception{
        String filepath = "C:/Test/sales_derby.json";
        Statement s = null;
        String antwort = "";

        String loadSimpleJSON = "call syscs_util.syscs_register_tool( 'simpleJson', true )";

        String createJsonTable = "create table jsonTable (json varchar(20000))";

        String createJson = "insert into jsonTable values(arrayToClob(toJSON('SELECT * FROM sales.verkaeufe')))";

        String fileQuery ="CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE(null,'JSONTABLE'," +
        "'"+filepath+"',',',null,'UTF-8', 'c:/data/export.dat')";

        String getJson = "SELECT * FROM jsonTable";

        try {
            s = conn.createStatement();
        
            s.execute(loadSimpleJSON);
            s.execute(createJsonTable);
            s.execute(createJson);
        }catch(Exception e){
            e.printStackTrace();
        }

        if (create)
        {
            try{
                s.execute(fileQuery);
                antwort = "JSON-Datei erfolgreich erstellt";
            }catch(Exception e){
                e.printStackTrace();
                antwort = "JSON-Datei erfolglos erstellt";
            }
        }
        else
        {
            queryList.add(getJson);
            antwort = writeToString(queryList);
        }
        return antwort;
        }

    /**
     * Erstellt Query um Rekursiv Befehl mit WITH-Klausel (Common Table Expression) auszuführen.
     * Es soll nach Unterstellten des Mitarbeiters mit der ID 7 gesucht werden und nach den Unterstellten der Unterstellten
     * Fügt die Query zu queryList hinzu und ruft anschließend die writeToList-Methode auf
     * @return Aufruf der Methode writeToList
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public ArrayList<String> recursive()throws Exception{
        String recursiveQuery = "with recursive hierarchie (id, name, manager_id, Abteilung) as " + //Erstellt Common Table Expression mit with und recursive
        "(select id, name, manager_id, Abteilung " +
        "FROM sales.Mitarbeiter WHERE ID = 7 " + //Definiert "Beginn" der rekursiven Suche mit ID 7
        "UNION " +
        "SELECT E.id, E.name, E.manager_id, E.Abteilung " + //Erstellt 2. Resultset in welchem rekursiv gesucht wird
        "FROM hierarchie H " +
        "JOIN sales.Mitarbeiter E on H.id = E.manager_id) " + //So lang es ID gibt, welche auch Manager_ID ist, wird weitergesucht
        "SELECT * FROM hierarchie";

        queryList.add(recursiveQuery);
        return writeToList(queryList);
    }

    /**
     * Erstellt Query um Rekursiv Befehl zu simulieren.
     * Es soll nach Unterstellten des Mitarbeiters mit der ID 7 gesucht werden und nach den Unterstellten der Unterstellten
     * Fügt die Query zu queryList hinzu und ruft anschließend die writeToList-Methode auf
     * @return Aufruf der Methode writeToList
     * @throws Exception Gibt Exception weiter, sofern eine geworfen wurde
     */
    public ArrayList<String> fake_recursive_query()throws Exception{
        String transitiveQuery = "SELECT ID, Name, Manager_id, Abteilung " +
        "FROM sales.Mitarbeiter " +
        "WHERE ID = 7 " + //Ermittelt Mitarbeiter mit ID 7
        "UNION " +
        "SELECT ID, Name, manager_id, Abteilung " + //Ermittelt direkte Unterstellte der ID 7
        "FROM sales.Mitarbeiter " +
        "WHERE Manager_ID = 7 " +
        "UNION " +
        "SELECT v1.ID, v1.Name, v1.manager_id, v1.Abteilung " + //Ermittelt indirekte Unterstellte der ID 7
        "FROM sales.Mitarbeiter v1, sales.Mitarbeiter v2 " +
        "WHERE v1.Manager_ID = v2.ID AND " +
        "v2.Manager_ID = 7";
        
        queryList.add(transitiveQuery);
        return writeToList(queryList);
    }

    /**
     * Testet die ROW_Number Funktion. Es sollen die jeweiligen 3 Städte innerhalb der jeweiligen 3 Länder basierend auf der Verkaufsmenge durchnummeriert werden.
     * Da innerhalb der Länder durchnummeriert wird, müssen die jeweiligen 3 Städte jeweils die 1-3 erhalten.
     * 
     * Ruft bei HSQLDB die ROW_Number FUnktion ohne Window FUnktion und somit auch ohne partition by Funktion auf, da diese nicht angeboten wird.
     * Ruft bei Derby die ROW_Number FUnktion mit Window FUnktion, jedoch ohne partition by Funktion auf, da diese nicht angeboten wird.
     * Ruft bei H2SQL die ROW_Number FUnktion mit Window FUnktion und mit partition by Funktion auf.
     * 
     * Die Ergebnisse werden mit dem String "rightString" verglichen
     */
    @Test
    @Order(1) 
    void teste_Window_and_Row_Number_Functions() throws Exception{ 
        String rightString = "AsienChinaShanghai30001\n"+
        "AsienChinaPeking50002\n"+
        "AsienChinaHong Kong70003\n"+
        "EuropaUKLondon70001\n"+
        "EuropaUKManchester120002\n"+
        "EuropaFrankfreichParis120003\n"+
        "Nord AmerikaKanadaMontreal50001\n"+
        "Nord AmerikaKanadaToronto100002\n"+
        "Nord AmerikaKanadaVancouver150003";

        String rightStringDerby = "AsienChinaShanghai30001\n"+
        "AsienChinaPeking50002\n"+
        "AsienChinaHong Kong70003\n"+
        "EuropaUKLondon70001\n"+
        "EuropaFrankfreichParis120002\n"+
        "EuropaUKManchester120003\n"+
        "Nord AmerikaKanadaMontreal50001\n"+
        "Nord AmerikaKanadaToronto100002\n"+
        "Nord AmerikaKanadaVancouver150003";

        if(currentDatabase.equals("hsql")){

            // Ausgabe
            System.out.println("------------------------Beginn Teste Window und Row Number HSQLDB------------------------");
            System.out.println(rightString);
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(row_number_with_window_function_without_partition());
            System.out.println("-------------------------Ende Teste Window und Row Number HSQLDB-------------------------");

            assertEquals(rightString,row_number_with_window_function_without_partition());

                 
        }
        else if(currentDatabase.equals("h2")){
                
            // Ausgabe
            System.out.println("------------------------Beginn Teste Window und Row Number H2SQL------------------------");
            System.out.println(rightString);
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(row_number_with_window_function_and_partition());
            System.out.println("-------------------------Ende Teste Window und Row Number H2SQL-------------------------");

            assertEquals(rightString,row_number_with_window_function_and_partition());
        }
        else if(currentDatabase.equals("derby")){
                
            // Ausgabe
            System.out.println("------------------------Beginn Teste Window und Row Number Derby------------------------");
            System.out.println(rightStringDerby);
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(row_number_with_window_function_without_partition());
            System.out.println("-------------------------Ende Teste Window und Row Number Derby-------------------------");

            assertEquals(rightStringDerby,row_number_with_window_function_without_partition());
        }
    }

    /**
     * Ruft bei hsqldb die Grouping Sets-Query auf und vergleicht dessen Resultset mit dem Resultset der Simulation des SQL ohne Grouping Sets Anwendung.
     * Derby und h2 bieten keine Möglichkeit für Grouping Sets
     */
    @Test
    @Order(2)
    void teste_Grouping_Sets() throws Exception
    {
        if(currentDatabase.equals("hsql")){
                // Ausgabe
                System.out.println("------------------------Beginn Teste Grouping Sets HSQLDB------------------------");
                System.out.println(printList(grouping_sets()));
                System.out.println("----------------------------------------------------------------------------------");
                System.out.println(printList(fake_grouping_sets()));
                System.out.println("-------------------------Ende Teste Grouping Sets HSQLDB-------------------------");

                assertEquals(grouping_sets(),fake_grouping_sets());
        }
        else if(currentDatabase.equals("h2")){
            assertTrue(false);
        }
        else if(currentDatabase.equals("derby")){
            assertTrue(false);
        }
    }

    /**
     * Ruft bei hsqldb und derby die Rollup-Query auf und vergleicht dessen Resultset mit dem Resultset der Simulation des SQL ohne Rollup Anwendung.
     * h2 bietet keine Möglichkeit für Rollup
     */
    @Test
    @Order(3)
    void teste_ROLLUP() throws Exception
    {
        if(currentDatabase.equals("hsql")){

             // Ausgabe
             System.out.println("------------------------Beginn Teste Rollup HSQLDB------------------------");
             System.out.println(printList(rollup()));
             System.out.println("----------------------------------------------------------------------------------");
             System.out.println(printList(fake_rollup_query()));
             System.out.println("-------------------------Ende Teste Rollup HSQLDB-------------------------");

            assertTrue(rollup().equals(fake_rollup_query()));
        }
        else if(currentDatabase.equals("h2")){
                assertTrue(false);
        }
        else if(currentDatabase.equals("derby")){
                
            // Ausgabe
            System.out.println("------------------------Beginn Teste Rollup Derby------------------------");
            System.out.println(printList(rollup()));
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(printList(fake_rollup_query()));
            System.out.println("-------------------------Ende Teste Rollup Derby-------------------------");

           assertTrue(compareLists(rollup(),fake_rollup_query()));
        }        
    }

    /**
     * Ruft bei hsqldb und h2 die Rekursiv-Query mit WITH-Klausel auf und vergleicht dessen Resultset mit dem Resultset der Simulation des SQL ohne Rekursiv-Befehl Anwendung.
     * Derby bietet keine Möglichkeit für Rekursiv
     * BESONDERHEIT: Lediglich h2 beginnt mit dem Mitarbeiter 7 ganz oben und geht hierarchisch die Unterstellten durch. hsqldb und simulierte Rekursiv-Anfragen beginnen mit niedrigstem Unterstellem
     */
    @Test
    @Order(4)
    void teste_Rekursiv() throws Exception{
        if(currentDatabase.equals("hsql")){

            // Ausgabe
            System.out.println("------------------------Beginn Teste Rekursion HSQLDB------------------------");
            System.out.println(printList(recursive()));
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(printList(fake_recursive_query()));
            System.out.println("-------------------------Ende Teste Rekursion HSQLDB-------------------------");
           
            assertEquals(recursive(), fake_recursive_query());
        }
        else if(currentDatabase.equals("h2")){
           
            // Ausgabe
            System.out.println("------------------------Beginn Teste Rekursion H2SQL------------------------");
            System.out.println(printList(recursive()));
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(printList(fake_recursive_query()));
            System.out.println("-------------------------Ende Teste Rekursion H2SQL-------------------------");

            assertTrue(compareLists(recursive(), fake_recursive_query())); //Nutze compareLists-Methode um Reihenfolge aufzulösen. h2 stellt als einziges hierarchisch korrekt dar
        }
        else if(currentDatabase.equals("derby")){
            assertTrue(false);
        }
    }

    @Test
    @Order(5)
    void teste_JSON_Datentyp() throws Exception
    {
        String rightJsonString = "[{\"Kontinent\":\"Nord Amerika\",\"Land\":\"Kanada\",\"Stadt\":\"Toronto\",\"menge_Verkaeufe\":10000},{\"Kontinent\":\"Nord Amerika\",\"Land\":\"Kanada\",\"Stadt\":\"Montreal\",\"menge_Verkaeufe\":5000},{\"Kontinent\":\"Nord Amerika\",\"Land\":\"Kanada\",\"Stadt\":\"Vancouver\",\"menge_Verkaeufe\":15000},{\"Kontinent\":\"Asien\",\"Land\":\"China\",\"Stadt\":\"Hong Kong\",\"menge_Verkaeufe\":7000},{\"Kontinent\":\"Asien\",\"Land\":\"China\",\"Stadt\":\"Peking\",\"menge_Verkaeufe\":5000},{\"Kontinent\":\"Asien\",\"Land\":\"China\",\"Stadt\":\"Shanghai\",\"menge_Verkaeufe\":3000},{\"Kontinent\":\"Europa\",\"Land\":\"UK\",\"Stadt\":\"London\",\"menge_Verkaeufe\":7000},{\"Kontinent\":\"Europa\",\"Land\":\"UK\",\"Stadt\":\"Manchester\",\"menge_Verkaeufe\":12000},{\"Kontinent\":\"Europa\",\"Land\":\"Frankfreich\",\"Stadt\":\"Paris\",\"menge_Verkaeufe\":12000}]";
            
        if(currentDatabase.equals("h2")){
            String jsonString = JSON_h2(false);

            //Ausgabe
            System.out.println("------------------------Beginn Teste JSON-Datentyp H2SQL------------------------");
            System.out.println(jsonString);
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(rightJsonString);
            System.out.println("-------------------------Ende Teste JSON-Datentyp H2SQL-------------------------");

            assertEquals(rightJsonString, jsonString);
        }
        else if(currentDatabase.equals("hsql")){
            assertTrue(false);
        }
        else if(currentDatabase.equals("derby")){
            String jsonString = JSON_derby(false);
            String derbyString = "[{\"KONTINENT\":\"Nord Amerika\",\"MENGE_VERKAEUFE\":10000,\"LAND\":\"Kanada\",\"STADT\":\"Toronto\"},{\"KONTINENT\":\"Nord Amerika\",\"MENGE_VERKAEUFE\":5000,\"LAND\":\"Kanada\",\"STADT\":\"Montreal\"},{\"KONTINENT\":\"Nord Amerika\",\"MENGE_VERKAEUFE\":15000,\"LAND\":\"Kanada\",\"STADT\":\"Vancouver\"},{\"KONTINENT\":\"Asien\",\"MENGE_VERKAEUFE\":7000,\"LAND\":\"China\",\"STADT\":\"Hong Kong\"},{\"KONTINENT\":\"Asien\",\"MENGE_VERKAEUFE\":5000,\"LAND\":\"China\",\"STADT\":\"Peking\"},{\"KONTINENT\":\"Asien\",\"MENGE_VERKAEUFE\":3000,\"LAND\":\"China\",\"STADT\":\"Shanghai\"},{\"KONTINENT\":\"Europa\",\"MENGE_VERKAEUFE\":7000,\"LAND\":\"UK\",\"STADT\":\"London\"},{\"KONTINENT\":\"Europa\",\"MENGE_VERKAEUFE\":12000,\"LAND\":\"UK\",\"STADT\":\"Manchester\"},{\"KONTINENT\":\"Europa\",\"MENGE_VERKAEUFE\":12000,\"LAND\":\"Frankfreich\",\"STADT\":\"Paris\"}]";

            //Ausgabe
            System.out.println("------------------------Beginn Teste JSON-Datentyp Derby------------------------");
            System.out.println(jsonString);
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(rightJsonString);
            System.out.println("-------------------------Ende Teste JSON-Datentyp Derby-------------------------");

            assertEquals(derbyString,jsonString);
        }
    }

    /**
     * Ruft bei h2 die Methode auf um eine JSON-Datei zu erstellen und vergleicht diese JSON-Datei mit einem bereits vorhandenen Vergleichs-String
     * hsqldb bietet keine Möglichkeit für JSON
     * Ruft bei Derby die Methode auf um ein JSON-File zu erstellen. Wie in der Methode beschrieben, wird die entsprechende Exception geworfen und empfangen sowie geprüft,
     * ob es sich um die richtige Exception mit entsprechendem Text handelt
     */
    @Test
    @Order(6)
    void teste_JSON_Export() throws Exception
    {
        String rightJsonString = FileUtils.readFileToString(new File("C:/Test/Vergleichs.json"), "utf-8");;

        if(currentDatabase.equals("h2")){
            JSON_h2(true);
            
            String jsonString = FileUtils.readFileToString(new File("C:/Test/sales_h2.json"), "utf-8");

            //Ausgabe
            System.out.println("------------------------Beginn Teste JSON-Datentyp schreiben H2SQL------------------------");
            System.out.println(jsonString);
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(rightJsonString);
            System.out.println("-------------------------Ende Teste JSON-Datentyp schreiben H2SQL-------------------------");
            
            assertEquals(rightJsonString,jsonString);
        }
        else if(currentDatabase.equals("hsql")){
            assertTrue(false);
        }
        else if(currentDatabase.equals("derby")){

            assertTrue(false);

            /** Format nach Export nicht korrekt, daher auskommentiert
            JSON_derby(true);
            File jsonFile = new File("C:/Test/sales_derby.json");
            
            String jsonString = FileUtils.readFileToString(jsonFile, "utf-8");
            String rightJsonString = FileUtils.readFileToString(rightJSON, "utf-8");

            //Ausgabe
            System.out.println("------------------------Beginn Teste JSON-Datentyp schreiben Derby------------------------");
            System.out.println(jsonString);
            System.out.println("----------------------------------------------------------------------------------");
            System.out.println(rightJsonString);
            System.out.println("-------------------------Ende Teste JSON-Datentyp schreiben Derby-------------------------");
            
            assertTrue(rightJsonString.equalsIgnoreCase(jsonString));*/
        }
    }

    /**
     * Ruft bei h2 die Methode auf um eine XML-Datei zu erstellen und vergleicht diese XML-Datei mit einer bereits vorhandenen Vergleichs-Datei
     * hsqldb bietet keine Möglichkeit für JSON
     * Ruft bei Derby die Methode auf um eine XML-Tabelle zu generien und prüft den letztendlichen Output gegen den String "rightXmlString"
     */
    @Test
    @Order(7)
    void teste_xml() throws Exception
    {
        if(currentDatabase.equals("h2")){
            xml_h2();

            String xmlString = FileUtils.readFileToString(new File("C:/Test/sales.xml"), "utf-8");
            String rightXmlString = FileUtils.readFileToString(new File("C:/Test/vergleichs.xml"), "utf-8");
            assertEquals(xmlString, rightXmlString);
        }
        else if(currentDatabase.equals("derby")){
            String rightXmlString = "<Verkauf><Kontinent>Nord Amerika</Kontinent><Land>Kanada</Land><Stadt>Toronto</Stadt><Menge>10000</Menge></Verkauf>\n"+
                "<Verkauf><Kontinent>Nord Amerika</Kontinent><Land>Kanada</Land><Stadt>Montreal</Stadt><Menge>5000</Menge></Verkauf>\n"+
                "<Verkauf><Kontinent>Nord Amerika</Kontinent><Land>Kanada</Land><Stadt>Vancouver</Stadt><Menge>15000</Menge></Verkauf>\n"+
                "<Verkauf><Kontinent>Asien</Kontinent><Land>China</Land><Stadt>Hong Kong</Stadt><Menge>7000</Menge></Verkauf>\n"+
                "<Verkauf><Kontinent>Asien</Kontinent><Land>China</Land><Stadt>Peking</Stadt><Menge>5000</Menge></Verkauf>\n"+
                "<Verkauf><Kontinent>Asien</Kontinent><Land>China</Land><Stadt>Shanghai</Stadt><Menge>3000</Menge></Verkauf>\n"+
                "<Verkauf><Kontinent>Europa</Kontinent><Land>UK</Land><Stadt>London</Stadt><Menge>7000</Menge></Verkauf>\n"+
                "<Verkauf><Kontinent>Europa</Kontinent><Land>UK</Land><Stadt>Manchester</Stadt><Menge>12000</Menge></Verkauf>\n"+
                "<Verkauf><Kontinent>Europa</Kontinent><Land>Frankfreich</Land><Stadt>Paris</Stadt><Menge>12000</Menge></Verkauf>";

            String xmlString = xml_derby();

                System.out.println(xmlString);
            assertEquals(rightXmlString,xmlString);
        }
        else if(currentDatabase.equals("hsql")){
            assertTrue(false);
        }
    }
}