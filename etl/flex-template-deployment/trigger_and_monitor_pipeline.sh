#!/bin/bash
# Trigger the Azure Pipeline for the Development Branch
trigger_response=$(curl -s -X POST "$AZURE_BASE_URL" \
   -H "Authorization: Bearer $AZURE_DEVOPS_PAT" \
   -H "Content-Type: application/json" \
   -d '{
           "resources": {
               "repositories": {
                   "self": {
                       "refName": "'refs/heads/$BRANCH'"
                   }
               }
           },
           "templateParameters": {"artifactName": "'$1'", "templateDirectory": "'$2'", "registryName":"'$GCR_REPO'", "variableGroup":"'$VARIABLE_GROUP'"}
       }')
# Debug: Log the response
echo "Trigger Response: $trigger_response" >&2
# Extract the Run ID from the trigger response
pipeline_run_id=$(echo "$trigger_response" | jq -r '.id')
if [ -z "$pipeline_run_id" ] || [ "$pipeline_run_id" == "null" ]; then
   echo "Error: Failed to retrieve Pipeline Run ID. Response: $trigger_response" >&2
   exit 1
fi
echo "Triggered Azure Pipeline for the Development branch with Run ID: $pipeline_run_id"

# Define the status URL with the Run ID
status_url="${AZURE_STATUS_URL/RUN_ID/$pipeline_run_id}"

# Poll for the pipeline state
state="unknown"
timeout=1200  # Timeout in seconds (20 minutes)
elapsed=0
interval=20  # Polling interval in seconds
echo "Monitoring Azure Pipeline state..."
while [[ "$state" != "completed" && $elapsed -lt $timeout ]]; do
   state_response=$(curl -s "$status_url" \
       -H "Authorization: Bearer $AZURE_DEVOPS_PAT")
   # Debug: Log the state response

   state=$(echo "$state_response" | jq -r '.state')
   result=$(echo "$state_response" | jq -r '.result')
   echo "PipelineRunId : $pipeline_run_id Current state : $state"
   if [[ "$state" == "completed" ]]; then
       echo "Pipeline completed with result: $result"
       if [[ "$result" == "succeeded" ]]; then
           exit 0
       else
           echo "Pipeline failed with result: $result"
           exit 1
       fi
   fi
   sleep $interval
   elapsed=$((elapsed + interval))
done
# Timeout condition
if [[ "$state" != "completed" ]]; then
   echo "Timeout reached without completing the pipeline."
   exit 1
fi