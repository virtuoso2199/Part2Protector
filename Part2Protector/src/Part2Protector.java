
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.xml.sax.InputSource;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
//JDBC includes
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.nio.file.*;



/**
* <h1>Part2Protector for filtering VHIE ADT/CCD files containing PHI protected by 42 CFR Part 2</h1>
* <p>
* This is a simple program that monitors the contents of a given directory for the addition of files
* When files are added, it determines, first by extension and then by contents, whether they are 
* ADT or CCD messages. If they are, it looks for the patient's medical record number in PID3 (for ADT)
* or in the appropriate node in the CCD file and searches for a match in a Part 2 Patient Inventory file.
* If a match is found (indicating the patient's health information is protected by 42 CFR Part 2, the file is deleted.
* Otherwise, the file is moved to the directory where the iNexx agent can upload it to the VHIE
*
* @author Jonathan Bowley
* @version 1.0
*/

public class Part2Protector {
    
   /**
    * Main checks for files in program directory every 30 seconds and loads into ArrayList. 
    * Parses MRN's from ADT & CCD files and compares to inventory of patients in Part 2 program as listed in inventoryFile
    * If ADT/CCD file contains MRN in inventoryFile, file is deleted. Otherwise, file is moved to uploadFileDir to await upload to VHIE via iNexx agent
    * When complete, sleeps for 30 seconds before trying again. 
    *
    * @param args   Not used
    * @author Jonathan Bowley
    * @see getADTMRN (File filename, segment)   parses MRN from HL7 (text) based ADT file
    * @see getCCDMRN (File filename)            parses MRN from XML based CCD file
    */
    
    //declare all globally accessible variables
	//Directory to check in 
    int filesDeleted = 0;
    int filesUploaded = 0;
    String inventoryFile = "C:\\Part2Protector\\inventory.txt";
    String dropFileDir = "C:\\Mirth_Staging\\"; 
    String CCDUploadFileDir="C:\\vitl\\ccdout\\";
    String ADTUploadFileDir="C:\\vitl\\adtout\\";
    int segment = 3; //PID segment to use for MRN
    boolean keepGoing = false; //processing loop control variable (starts with loop stopped)
    
	//declare GUI components
    JFrame frame;
    JPanel pnlBtnControls;
    JPanel pnlStatus;
    JTextArea txtAreaStatus;
    JButton btnStart;
    JButton btnStop;
    JLabel lblADTProcessed;
    JLabel lblCCDProcessed;
    JLabel lblFilesDeleted;
    JLabel lblFilesUploaded;
    List<String> MRNList = new ArrayList<String>();
    File fileDir;
    
    
    public static void main(String[] args) {
       Part2Protector gui = new Part2Protector();
       gui.setupGUI();
       gui.go();
    }
    
    public void setupGUI(){
    	frame = new JFrame("Part2Protector");
    	frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    	pnlBtnControls = new JPanel();
    	pnlStatus = new JPanel();
    	txtAreaStatus = new JTextArea();
    	btnStart = new JButton("Start");
    	btnStart.addActionListener(new startListener());
    	btnStop = new JButton("Stop");
    	btnStop.addActionListener(new stopListener());
    	lblADTProcessed = new JLabel ("ADT Messages Processed: 0");
    	lblCCDProcessed = new JLabel ("CCD Messages Processed: 0");
    	lblFilesDeleted = new JLabel ("Files Deleted: 0");
    	lblFilesUploaded = new JLabel ("Files Uploaded: 0");
    	JScrollPane scroller = new JScrollPane(txtAreaStatus);
    	scroller.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
    	scroller.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
    	
    	//add buttons to pnlBtnControls
    	pnlBtnControls.add(btnStart);
    	pnlBtnControls.add(btnStop);
    	
    	//Add file counters to status panel
    	pnlStatus.setLayout(new BoxLayout(pnlStatus,BoxLayout.Y_AXIS));
    	pnlStatus.add(lblADTProcessed);
    	pnlStatus.add(lblCCDProcessed);
    	pnlStatus.add(lblFilesDeleted);
    	pnlStatus.add(lblFilesUploaded);
    	
    	frame.getContentPane().add(BorderLayout.NORTH,pnlBtnControls);
    	frame.getContentPane().add(BorderLayout.WEST, pnlStatus);
    	frame.getContentPane().add(BorderLayout.CENTER,scroller);
    	frame.setSize(500, 500);
    	frame.setVisible(true);
    }
    
    public void go(){
    	//setup GUI
    	
               
        
        
        
        //load MRN's of Part 2 patients into MRNList ArrayList from result set
        MRNList = getMRNList();
        fileDir = new File (dropFileDir);

/*
       //Load MRN's of Part 2 patients into ArrayList 
        BufferedReader in = null;
        
        
        try {   
            in = new BufferedReader(new FileReader(inventoryFile));
            String str;
            while ((str = in.readLine()) != null) {
                MRNList.add(str.trim());
            }
        } catch (IOException e) {
                e.printStackTrace();
        } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
        }
    */    
        //DEBUG ONLY: Output list of MRN's to console
        txtAreaStatus.append("MRN's in list:\n");
        for (String s: MRNList){
            txtAreaStatus.append(s+"\n");
        }
        
        txtAreaStatus.append(MRNList.size()+" patients in inventory\n");
        
    }
    
    public class msgProcessor implements Runnable{
    	
    	public void run(){
        //Load file contents in fileDir 
        ArrayList<File> filenames = new ArrayList();
        ArrayList<File> ADTFiles = new ArrayList();
        ArrayList<File> CCDFiles = new ArrayList();
        ArrayList<File> filesToDelete = new ArrayList();
        ArrayList<File> CCDFilesToUpload = new ArrayList();
        ArrayList<File> ADTFilesToUpload = new ArrayList();
        //DEBUG ONLY: Loop through files and output names to console
        
        //start of infinite processing loop
        while (keepGoing){
        MRNList = getMRNList();
        
        txtAreaStatus.append(MRNList.size()+" patients in inventory\n");
        filenames = new ArrayList(Arrays.asList(fileDir.listFiles()));
        txtAreaStatus.append("\n\nFiles to process:\n");
        for (File f: filenames) { 
            String extension = f.getName().substring(f.getName().length()-3);
            if (extension.equals("xml")){ //add all .xml files to CCD files list
                CCDFiles.add(f);
            } else if (extension.equals("hl7")){ //add all .hl7 files to ADT files list
                ADTFiles.add(f);
            } else { //add files with any other extension to the discard pile
                txtAreaStatus.append(f.getName()+" is not a recognized type. Skipping...\n");
            } 
    

	}
        for (File f:ADTFiles){ //Iterate through all ADT files found
            boolean isPart2=false;
            txtAreaStatus.append(f.getName()+"\n");
            String ADTMRN = getADTMRN(f,segment); //parse MRN from ADT file using current file and segment identified at beginning of class
            txtAreaStatus.append("MRN is: "+ ADTMRN); //Display MRN on console
            for (String s: MRNList){ //iterate through list of Part 2 MRN's 
                if (s.equals(ADTMRN)){ //compare ADT file's MRN to current MRN in Part 2 array and flip isPart2 switch to true
                    txtAreaStatus.append("\nPART 2 PROTECTED!!");
                    isPart2=true;
                } 
            }
            
            if (isPart2){ //if current file is Part 2 protected, add to list of files to delete
                txtAreaStatus.append("\nFile "+f.getName()+" will be deleted\n"); //DEBUG ONLY
                //add logic to delete file
                filesToDelete.add(f);
                filesDeleted +=1;
            } else { //if isPart2 is false, patient is not in Part 2 program and file can be moved to directory from which iNexx uploads by being added to list of files to upload
                txtAreaStatus.append("\nFile "+f.getName()+" will be uploaded\n");
                ADTFilesToUpload.add(f);
                filesUploaded +=1;
                
            }
            
        }
       
       
        for (File f:CCDFiles){ // Iterate through CCD files
            boolean isPart2=false; //assume file is not Part 2 protected by default
            txtAreaStatus.append(f.getName()+"\n");
            String CCDMRN = getCCDMRN(f);
            txtAreaStatus.append("MRN is: "+ CCDMRN+"\n");
            for (String s: MRNList){
                if (s.equals(CCDMRN)){
                    txtAreaStatus.append("PART 2 PROTECTED!!\n");
                    isPart2=true;
                } 
            }
            
            if (isPart2){
                txtAreaStatus.append("File "+f.getName()+" will be deleted\n"); 
                //add logic to delete file
                filesToDelete.add(f);
                filesDeleted +=1;
            } else if (CCDMRN ==""){
                //if MRN is blank, error occurred. Fail safe and delete message.
                txtAreaStatus.append("MRN UNKNOWN!!\n");
                txtAreaStatus.append("File "+f.getName()+" will be deleted\n"); 
                //add logic to delete file
                filesToDelete.add(f);
                filesDeleted +=1;
            }else{
                txtAreaStatus.append("File "+f.getName()+" will be uploaded\n");
                //add logic to move file to upload directory
                CCDFilesToUpload.add(f);
                filesUploaded +=1;
                //need logic to overwrite if file already exists in directory
            }
        }
        
        //show summary of this run and since program started
        lblADTProcessed.setText("ADT Files Processed: "+ADTFiles.size()); //display count of ADT files processed
        lblCCDProcessed.setText("CCD Files Processed: "+CCDFiles.size()); 
        txtAreaStatus.append(filesToDelete.size()+" files to delete\n");
        txtAreaStatus.append(CCDFilesToUpload.size()+" CCD files to upload\n");
        txtAreaStatus.append(ADTFilesToUpload.size()+" ADT files to upload\n");
        lblFilesDeleted.setText("Files Deleted: "+filesDeleted);
        lblFilesUploaded.setText("Files Uploaded: "+filesUploaded);
        //Delete files in filesToDelete arrayList
        txtAreaStatus.append("Deleting protected files...\n");
        for (File f:filesToDelete){
            txtAreaStatus.append(f.getName()+"\n");
            f.delete();
        }
        
        //Move CCD files in CCDFilesToUpload arrayList to uploadFileDir
        txtAreaStatus.append("Moving CCD files to drop directory...\n");
        for (File f:CCDFilesToUpload){
            Path CCDSource = f.toPath(); 
            Path CCDDestination = new File(CCDUploadFileDir+f.getName()).toPath();
            try {
                    if( Files.move(CCDSource,CCDDestination,StandardCopyOption.REPLACE_EXISTING)!=null)
                    {
                        txtAreaStatus.append(CCDUploadFileDir+f.getName()+" moved successfully\n");
                    } else {
                        txtAreaStatus.append(CCDUploadFileDir+f.getName()+" MOVE FAILED!!\n");
                    }//need to add logic for occassions when filename exists
                } catch (Exception e){
                    e.printStackTrace();
                }
        }
        
        //Move ADT files in ADTFilesToUpload arrayList to uploadFileDir
        txtAreaStatus.append("Moving ADT files to drop directory...\n");
        for (File f:ADTFilesToUpload){
            Path ADTSource = f.toPath(); 
            Path ADTDestination = new File(ADTUploadFileDir+f.getName()).toPath();
            try {
                    
                    if(Files.move(ADTSource,ADTDestination,StandardCopyOption.REPLACE_EXISTING)!=null)
                    {
                        txtAreaStatus.append(ADTUploadFileDir+f.getName()+" moved successfully\n");
                    } else {
                        txtAreaStatus.append(ADTUploadFileDir+f.getName()+" MOVE FAILED!\n");
                    }//need to add logic for occassions when filename exists
                } catch (Exception e){
                    e.printStackTrace();
                }
        }
        
       
       
       //empty ArrayLists for next go round
        filenames.clear();
        ADTFiles.clear();
        CCDFiles.clear();
        filesToDelete.clear();
        CCDFilesToUpload.clear();
        ADTFilesToUpload.clear();
        
        //Sleep 30 seconds and start again
       txtAreaStatus.append("Sleeping 30 seconds...\n\n");
        try {
               Thread.sleep(30000);
        } catch(InterruptedException ex) {
               Thread.currentThread().interrupt();
        }
        
        txtAreaStatus.append("Waking up for next loop...\n");
      }
    }
    }
    
    class startListener implements ActionListener{
    	public void actionPerformed(ActionEvent event){
    		txtAreaStatus.append("\nProcessing started!\n");
    		start();
    		txtAreaStatus.append("keepGoing = "+keepGoing+"\n");
            //go();
    	}
    }
    
    class stopListener implements ActionListener{
    	public void actionPerformed(ActionEvent event){
    		txtAreaStatus.append("\nProcessing stopped!\n");
    		stop();
    		txtAreaStatus.append("keepGoing = "+keepGoing+"\n");
    	}
    }
    
    public void stop(){
    	keepGoing = false;
    }
    
    public void start(){
    	keepGoing = true;
    	Thread processingThread = new Thread(new msgProcessor());
        processingThread.start();
    }
    
    /**
     * getADTMRN takes a file object and a integer segment number and parses a medical record number (MRN) from the file
     * 
     * @param ADTFile   file object that points to ADT file (HL7 file written in text)
     * @param segment   integer of PID segment in HL7 message where MRN is kep
     * @return MRN      String containing medical record number from HL7 message 
     */
    public static String getADTMRN(File ADTFile, int segment){ //get file name and PID segment to look in
        String MRN=""; //String to hold MRN
        try{
            
            String content = new Scanner(ADTFile).useDelimiter("\\Z").next(); //load ADT file contents into string for procesing
            //System.out.println("ADT Message: "+content);
            int PIDPosition = content.indexOf("PID"); //find location of PID line in file contents loaded into string
            //System.out.println("PID location is"+ PIDPosition); //DEBUG ONLY
            int startPosition; //placeholder for current read position while moving through file
            startPosition = content.indexOf("|",PIDPosition+1); //find first pipe before entering loop
           // System.out.println("First pipe found at: "+ startPosition); //DEBUG ONLY
            for(int i=0;i<segment-1;i++){
                startPosition = content.indexOf("|",startPosition+1); //iterate through pipes until specified segment is reached
                //System.out.println("Pipe "+(i+1)+" found at: "+startPosition); //DEBUG ONLY
            }
            int endPosition = content.indexOf("|", startPosition+1); //find position of end of segment
            //System.out.println("End position of PID "+segment+" found at: "+endPosition); //DEBUG ONLY
            MRN = content.substring(startPosition+1,endPosition);
            //System.out.println("MRN is: "+MRN); //DEBUG ONLY
            
            
        } catch (Exception e){
            e.printStackTrace();
        }
        
        
        return MRN;
       }
    
    /**
     * getCCDMRN takes File object and returns medical record number (MRN) as defined in HL7 CCD standard
     * 
     * @param CCDFile   File object that points to CCD file (XML file that is formatted in HL7's CCD standard)
     * @return MRN      returns a String containing the parsed medical record number (MRN)
     */
    
    public static String getCCDMRN(File CCDFile){ //reads CCD (XML file), extracts "id" node value and returns as string
        String MRN =""; //string to hold MRN
        
        try {
            DocumentBuilderFactory xmlFact = DocumentBuilderFactory.newInstance();
            xmlFact.setNamespaceAware(false);
            DocumentBuilder builder = xmlFact.newDocumentBuilder();
            XPath xpath = XPathFactory.newInstance().newXPath();
            InputStream CCDStream = new FileInputStream(CCDFile);
            InputSource source = new InputSource(CCDStream);
            Document doc = builder.parse(source);
            String expr = "/ClinicalDocument/recordTarget/patientRole/id/@extension";
            //MRN = xpath.evaluate("/ClinicalDocument/recordTarget/patientRole/id/@extension", source);
            MRN = (String) xpath.compile(expr).evaluate(doc ,XPathConstants.STRING);
            //System.out.println("MRN in method is: "+MRN); //DEBUG ONLY
        } catch (Exception e){
            e.printStackTrace();
        }
        
        return MRN; 
        
    }
    
    public static List getMRNList(){
        //Create list of medical record numbers
        List<String> PersonList = new ArrayList<String>();
        
        //connect to database
        Connection conn = null;
        
        try {
            
            String dbURL = "jdbc:sqlserver://ng01;DatabaseName=NGPROD";
            String user = "sa";
            String pass = "ngsachpw";
            conn = DriverManager.getConnection(dbURL, user, pass);
            if (conn != null) {
                DatabaseMetaData dm = (DatabaseMetaData) conn.getMetaData();
                System.out.println("Driver name: " + dm.getDriverName());
                System.out.println("Driver version: " + dm.getDriverVersion());
                System.out.println("Product name: " + dm.getDatabaseProductName());
                System.out.println("Product version: " + dm.getDatabaseProductVersion());
                
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(" SELECT DISTINCT  P.person_nbr FROM patient_diagnosis PD INNER JOIN user_mstr UM ON  PD.created_by = UM.user_id INNER JOIN person P ON PD.person_id = P.person_id INNER JOIN patient_status PS ON P.person_id = PS.person_id INNER JOIN patient_status_mstr PSM ON PS.patient_status_id = PSM.patient_status_id INNER JOIN provider_mstr PM ON UM.provider_id = PM.provider_id WHERE PD.icd9cm_code_id IN ('291.2', '291.81', '292.0','292.11', '292.81','292.84', '292.89', '292.9', '303.00', '303.90', '304.00', '304.10', '304.20', '304.21', '304.22', '304.23', '304.3', '304.31', '304.32', '304.33', '304.80', '305.00', '305.20', '305.30', '305.30', '305.40', '305.5', '305.50', '305.51', '305.52', '305.53', '305.6', '305.60', '305.61', '305.62', '305.63', '305.70', '305.90', '305.91', '305.92', '305.93','F91.3','F63.81','F91.1','F91.2','F91.9','F60.2','F63.1','F63.3','F91.8','F91.9','F10.10','F10.20','F10.129','F10.229','F10.929','F10.239','F10.232','F10.99','F15.929','F15.93','F15.99','F12.10','F12.20','F12.129','F12.229','F12.929','F12.122','F12.222','F12.922','F12.288','F12.99','F16.10','F16.20','F16.983','F16.99','F18.10','F18.20','F18.129','F18.229','F18.929','F18.99','F11.10','F11.20','F11.129','F11.229','F11.929','F11.122','F11.222','F11.922','F11.23','F11.99','F13.10','F13.20','F13.129','F13.229','F13.929',	'F13.239','F13.232','F13.99','F15.10','F14.10','F15.20','F14.20','F15.129','F15.229','F14.929','F15.122','F15.222','F15.922','F14.122','F14.222','F14.922','F15.23','F14.23','F15.99','F14.99','Z72.0','F17.200','F17.203','F17.209','F19.10','F19.20','F19.129','F19.229','F19.929','F19.239','F19.99') AND (PM.provider_subgrouping2_id ='41CA9A4B-004A-4478-9F38-54061DE3EA32' OR (PM.last_name LIKE 'Adams%' AND PM.first_name LIKE 'David%') OR (PM.last_name LIKE 'Brooklyn%' AND PM.first_name LIKE 'John%') OR (PM.last_name LIKE 'Fisher%' AND PM.first_name LIKE 'Pat%') OR (PM.last_name LIKE 'Inker%' AND PM.first_name LIKE 'Rachel%') OR (PM.last_name LIKE 'Sirois%' AND PM.first_name LIKE 'Michael%') OR (PM.last_name LIKE 'Stein%' AND PM.first_name LIKE 'Heather%') OR (PM.last_name LIKE 'Warnken%' AND PM.first_name LIKE 'Wayne%')OR (PM.last_name LIKE 'Willingham%' AND PM.first_name LIKE 'Jennifer%')) AND (P.last_name NOT LIKE 'Test%' OR P.last_name NOT LIKE 'Pooh%')");
                while (rs.next()){
                    //System.out.println(rs.getString("person_nbr").trim());
                    //MRNList.add(str.trim());
                    PersonList.add(rs.getString("person_nbr").trim());
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        
        return PersonList;
    }

}

