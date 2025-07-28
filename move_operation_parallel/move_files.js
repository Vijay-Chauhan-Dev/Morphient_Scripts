import 'dotenv/config';
import { 
    S3Client, 
    ListObjectsV2Command, 
    CopyObjectCommand, 
    DeleteObjectCommand 
} from "@aws-sdk/client-s3";


// Create an S3 client
const s3Client = new S3Client({
    region: "us-east-1",
    credentials: {
      accessKeyId: process.env?.accessKeyId,
      secretAccessKey: process.env?.secretAccessKey,
    },
  });
/**
 * Lists all object keys from a specific S3 prefix, handling pagination.
 * @param {string} bucket - The S3 bucket name.
 * @param {string} prefix - The prefix to filter objects by.
 * @returns {Promise<string[]>} - A promise that resolves to an array of object keys.
 */
async function listAllObjectKeys(bucket, prefix) {
    const keys = [];
    let isTruncated = true;
    let continuationToken;

    console.log(`Listing objects in bucket "${bucket}" with prefix "${prefix}"...`);

    while (isTruncated) {
        const params = {
            Bucket: bucket,
            Prefix: prefix,
        };
        if (continuationToken) {
            params.ContinuationToken = continuationToken;
        }

        try {
            const response = await s3Client.send(new ListObjectsV2Command(params));
            if (response.Contents) {
                response.Contents.forEach(item => {
                    // Don't move the "folder" placeholder object if it exists
                    if (item.Key !== prefix) {
                        keys.push(item.Key);
                    }
                });
            }
            isTruncated = response.IsTruncated;
            continuationToken = response.NextContinuationToken;
        } catch (err) {
            console.error("Error listing objects:", err);
            throw err;
        }
    }
    console.log(`Found ${keys.length} objects to move.`);
    return keys;
}

/**
 * Moves a single S3 object by copying it to a new key and deleting the original.
 * @param {string} bucket - The S3 bucket name.
 * @param {string} sourceKey - The original key of the object.
 * @param {string} destinationKey - The new key for the object.
 * @returns {Promise<void>}
 */
async function moveObject(bucket, sourceKey, destinationKey) {
    // Copy the object to the new location
    // const copyParams = {
    //     Bucket: bucket,
    //     CopySource: `${bucket}/${sourceKey}`,
    //     Key: destinationKey,
    // };
    // await s3Client.send(new CopyObjectCommand(copyParams));

    // Delete the original object
    const deleteParams = {
        Bucket: bucket,
        Key: sourceKey,
    };
    await s3Client.send(new DeleteObjectCommand(deleteParams));
}

/**
 * Main function to run the move operation.
 */
async function runMoveOperation() {
    // if (S3_CONFIG.bucket === "YOUR_BUCKET_NAME" || S3_CONFIG.region === "YOUR_AWS_REGION") {
    //     console.error("Please fill in your S3 bucket name and AWS region in the S3_CONFIG object.");
    //     return;
    // }

    try {
        console.log("Starting move operation...");
        let startTime = Date.now();
        // 1. Get all object keys from the source prefix
        const sourceKeys = await listAllObjectKeys(S3_CONFIG.bucket, S3_CONFIG.sourcePrefix);

        if (sourceKeys.length === 0) {
            console.log("No objects found to move.");
            return;
        }

        // 2. Create an array of move promises
        const movePromises = sourceKeys.map(sourceKey => {
            // Remove the source prefix from the key to get the base name
            const baseKey = sourceKey.substring(S3_CONFIG.sourcePrefix.length);
            const destinationKey = `${S3_CONFIG.backupPrefix}${baseKey}`;
            
            return moveObject(S3_CONFIG.bucket, sourceKey, destinationKey)
                .then(() => ({ status: 'fulfilled', value: `Moved ${sourceKey} to ${destinationKey}` }))
                .catch(err => ({ status: 'rejected', reason: `Failed to move ${sourceKey}: ${err.message}` }));
        });

        // 3. Execute all promises in parallel
        console.log(`Starting parallel move for ${movePromises.length} objects...`);
        const results = await Promise.allSettled(movePromises);

        // 4. Report the results
        let successCount = 0;
        let failureCount = 0;

        results.forEach(result => {
            if (result.status === 'fulfilled') {
                // console.log(result.value); // Uncomment for verbose success logging
                successCount++;
            } else {
                console.error(result.reason);
                failureCount++;
            }
        });

        console.log("\n--- Move Operation Complete ---");
        console.log(`Successfully moved: ${successCount} objects.`);
        console.log(`Failed to move:   ${failureCount} objects.`);
        let endTime = Date.now();
        console.log(`Total time taken: ${endTime - startTime} ms`);
        console.log("-----------------------------");

    } catch (err) {
        console.error("\nAn error occurred during the move operation:", err);
    }
}

let bucketList = [
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1381",
  ];
// --- Configuration ---
// s3://110ec58a-a0f2-4ac4-8393-c866d813b8d8-026/110ec58a-a0f2-4ac4-8393-c866d813b8d8#Account#0001/data/json/adsDataIPL2025/1742514514173/JITDelta-256nary/adsDataIPL2025JITDelta32/
let S3_CONFIG = {};

for(let bucket of bucketList){
    S3_CONFIG = {
        bucket: bucket, 
        sourcePrefix: "110ec58a-a0f2-4ac4-8393-c866d813b8d8#Account#0001/data/json/adsDataIPL2025JIT32/", 
        backupPrefix: "JITDeltaOAD-backup/GBValueAndDBCMappings/adsDataIPL2025JITDelta32/",           
        region: "us-east-1"          
    };  
    console.info(`Moving files from ${S3_CONFIG.bucket} to ${S3_CONFIG.backupPrefix}`);
    runMoveOperation();
    
    //wait for 25 seconds until next move operation
    await new Promise(resolve => setTimeout(resolve, 20*1000));
}

