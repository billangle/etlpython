
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  UpdateCommand
} from "@aws-sdk/lib-dynamodb";




export const handler = async (event) => {

  const jobDetails = event.logged.jobDetails;
  const jobId = event.step1Result.Output.jobId;
  const tableName = process.env.TABLE_NAME;
  const jobstate = event.glueResult.JobRunState;
  const project = process.env.PROJECT;
  const bucketRegion = process.env.BUCKET_REGION;

  const ddb = new DynamoDBClient({ region: bucketRegion });

  console.log("LogResults: event: " + JSON.stringify(event) + " jobId: " + jobId + " tableName: " + tableName + " project: " + project);

   let commandR;
        try {
        
            commandR = new UpdateCommand({
                TableName: tableName,
                Key: {
                  jobId: jobId,
                  project: project
                },
                UpdateExpression: "set jobState = :jobstate, fullResponse = :response",
                ExpressionAttributeValues: {
                  ":response": jobDetails,
                  ":jobstate": jobstate
                },
                ReturnValues: "ALL_NEW",
              });
            
             let res = await ddb.send(commandR);
        } catch (e) {
            console.error("Error updating row: " + jobId + " : " + JSON.stringify(commandR) + " e: " + e);
        }
  
  return event;
};
