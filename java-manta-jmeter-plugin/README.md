<h2> JMeter Manta Extension </h2>
 This porject has 2 different ways of running, the first it can be ran as a Maven build. This will start JMeter and run a simple test. The second item this will produce is a jar file that can be inserted into JMeter that can be used in creating test cases using JMeter's Java Request. Either way JMeter will use the shell variables MANTA_URL,MANTA_USER, and MANTA_KEY_ID and currently they can not be overridden.
 

<h3> Running from Maven </h3>
  To run this from maven simply clone a copy of the repository and run the command 'mvn verify', this should download and compile everything you will need and it will run JMeter and the simple test that is in the /src/test/jmeter directory. The test has a few parameters which can be set using the file jmeter.properties in the /src/test/jmeter directory. 
 
 The test will, for each thread, will craete a directory structure, and will then upload a file or set of files (if MPU is enabled),
 and do this a number of times. The test does not read the files back just uploads them. The root directory that it starts with is
 determined by date and time. 

 The test parameters are :

<table> 
   <tr> 
     <th> name </th><th> default value </th><th> description </th>  
   </tr>
   <tr>  
       <td>iteration</td>
       <td>1</td>  
       <td>Nuber of iterations per thread </td>  
  </tr>
   <tr>  
       <td>depth</td>
       <td>1</td>  
       <td>The depth of the directory structure to be used.</td>  
  </tr>
   <tr>  
       <td>thread_count</td>
       <td>1</td>  
       <td>Number of threads to be used</td>  
  </tr>
   <tr>  
       <td>encrypt</td>
       <td>true</td>  
       <td>Sets the encrypted flag in the Manta client</td>  
  </tr>
   <tr>  
       <td>upload</td>
       <td>true</td>  
       <td></td>  
  </tr>
   <tr>  
       <td>verify</td>
       <td>1</td>  
       <td>Sets the verify flag in the client</td>  
       </td> 
  </tr>
   <tr>  
       <td>multipart</td>
       <td>true</td>  
       <td></td>  
  </tr>
   <tr>  
       <td>cleanup</td>
       <td>false</td>  
       <td>When set the data that is created will be destroyed.</td>  
  </tr>
 </table>

<h3> Generating a jar for extending JMeter</h3>

This can be used to create a jar file that will go into the /lib/ext directory for JMeter, this is useful when you want to 
design a test using this extension.

Setup:  
  run the git clone on this repository
  run mvn package
  This should create a target directory, in the target directory, in it there should be JMeterMantaExtension-0.0.1-SNAPSHOT.jar	
  Move JMeterMantaExtension-0.0.1-SNAPSHOT.jar to the JMeter/libs/ext directory, note this is not a shaded jar so you will also need
  a copy of the Java-MantaClient. I would make this a shaded jar and include everything but it also requires a bunch of JMeter classes
  as well.
  
  Start JMeter
   1. Create a new thread
   2. Add a Java Sampler
   3. In the Java Sampler you will see the following : 
      <ol> 
       <li>CreateDirectory</li>
       <li>CreateObject</li>
       <li>DeleteDirectory</li>
       <li>DeleteObject</li>
       <li>DirectoryListing</li>
       <li>...</li>
       </ol>
   Each of these calls has it's own parameters, but the MANA_URL,MANTA_USER, and MANTA_KEY_ID will propagate from the enviromental variables 
   if they are set. 
   
   For most part they do nothing really special just call the equivellent calls in the Java Manta client. This was basically just an easy 
   way to propagte data into a uniform dashboard for my other tests.
   
  
