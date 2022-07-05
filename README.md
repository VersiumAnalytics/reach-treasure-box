# Consumer and Business Data Enrichment (Versium REACH APIs) 
 
This Treasure Box demonstrates how to leverage the Versium REACH APIs to retrieve third party data for consumers and businesses. This will allow access to all of the consumer and business data you need to integrate in your application development and enable your marketing to identify, understand and reach their ideal prospects.

## Prerequisites
A Versium REACH account and API key – see details on: https://api-documentation.versium.com/docs/start-building-with-versium 
Treasure Data account with a Master Segment containing profiles to be enriched.
 
**Note**: Treasure Data environment must be configured to allow running custom Python scripts.

## Installation steps
Install the treasure data tool belt on your local machine.
Once installed, open your command line client and authenticate with your Treasure Data account by running the command td account: 
``` 
td account
```
Download the reach-treasure-box Github repository then cd into the newly created directory
```
git clone git@github.com:VersiumAnalytics/reach-treasure-box.git
cd reach-treasure-box
```
Finally upload the reach_integration workflow to your Treasure Box account:
```
td wf push reach_integration
```
You can now login to your Treasure Data account and verify that the reach_integration workflow was successfully uploaded (insert image here).

## Configure the workflow
1) Open the reach_integration workflow then click the “Launch Project Editor” button to edit the reach_integration.dig file. 

2) Click the “Edit files” button and provide the following updates:
   
   a) Provide your Treasure Data API key and REACH API key.

   b) For REACH_API_NAME, specify which REACH API to query (this is the last portion of the Versium REACH API query url listed below):
     - contact
     - demographic
     - b2conlineaudience
     - b2bonlineaudience
     - firmographic
     - c2b
     - iptodomain

     Details on all REACH APIs and their usage can be  found here: https://api-documentation.versium.com/docs. 

     **Note**: the contact and demographic APIs require you to specify which categories of data to return in the response. 
               Provide a comma separated list of these outputs for the REACH_API_OUTPUTS value. 
               See documentation for more details: https://api-documentation.versium.com/docs
   
   c) For PROFILES_DATABASE and PROFILES_TABLE, enter the name of the master segment database and table to perform the enrichment on.

   d) For ENRICHED_PROFILES_TABLE, enter the name of the table to insert the enriched profiles into.

   e) If the ENRICHED_PROFILES_TABLE already exists and already contains enriched profiles that you don’t want to enrich again, enter the name of the column that      contains a unique identifier for the profiles in the PROFILE_ID_COL value. This column must exist in both the PROFILES_TABLE and the ENRICHED_PROFILES_TABLE.

   f) Finally, enter column names in the PROFILES_TABLE to use for matching against.

   g) There are some optional settings at the bottom of the file to control how the query is performed and what progress
      information is output. We recommend leaving these as the default values for most use cases.

3) After you have finished editing the reach_integration.dig file, click the “Save & Commit” button.

## Run the workflow
Simply click the “New Run” button to run the workflow. Once the job is complete, you can review the data in the newly created table, under the databases section. 

## Contact and Support
For any questions or issues please contact support@versium.com or visit www.versium.com.
