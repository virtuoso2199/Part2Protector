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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public static void main(String[] args){

/**
* Main checks for files in program directory every 30 seconds and loads into ArrayList. 
* Passes ADT files to ADTParser, passes CCD files to CCDParser, deletes all others. 
* When complete, checks directory for more files. If none, sleeps for 30 seconds before trying again. 
*
* @author Jonathan Bowley
* @param args[] 	Not used
* @see ADTParser (File filename)
*/

//Directory to check in 
File fileDir = new File ("C:\\Part2Protector\\");

//Load file contents in fileDir & output to screen (output for debugging only)
ArrayList<String> filenames = new ArrayList<String>(Arrays.asList(fileDir.list()));

for (String s: filenames) {
	System.out.println
	}

}

