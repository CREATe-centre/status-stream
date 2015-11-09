## Stream
This application utilises the Twitter4J library to access the Twitter streaming API and capture authenticated user activity to a database. It uses the *User Stream Listener* to listen to activity for the authenticated user and also *Status Stream Listener* filtered with the authenticated users' followers to listen to their related activity. 
Currently it stores the activity as raw JSON along with the authenticated user id and a timestamp of when the activity occurred.

## Setup
The project can be set up as a Maven project in Eclipse. The pom.xml file specifies the external libraries that the project utilises.

*GetStream.java* class requires the **consumer key** and **consumer secret** variables to be set. 

*CreateStream.java* class requires the **access token** and **access token secret** for the authenticating user and can be modified to take these arguments from the command line but currently they have to be hard coded in to the application. 

*DBConnect.java* contains the credentials to connect to the local mysql database. Modifiy the connection string with the name of the database and username nad password.

*wp_twitter_data.sql* is the script for creation of the database table that that application uses. 
