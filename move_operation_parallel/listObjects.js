import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { 
    S3Client, 
    ListObjectsV2Command
} from "@aws-sdk/client-s3";

// --- Configure dotenv to load .env from the parent directory ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, '../.env') });

// Create an S3 client
const s3Client = new S3Client({
    region: "us-east-1",
    credentials: {
      accessKeyId: process.env?.accessKeyId,
      secretAccessKey: process.env?.secretAccessKey,
    },
  });

let S3_CONFIG = {};
let count = 0;
let bucketList = [
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1381",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1382",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1383",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1387",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1385",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1384",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1388",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1386",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1376",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1389",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1390",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1391",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1392",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1393",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1394",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1395",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1396",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1397",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1398",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1399",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1400",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1401",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1402",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1403",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1404",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1405",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1407",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1406",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1409",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1408",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1410",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1411",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1412",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1413",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1414",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1415",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1416",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1417",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1418",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1419",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1420",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1421",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1422",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1423",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1425",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1426",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1427",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1428",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1424",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1429",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1430",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1431",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1432",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1433",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1434",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1436",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1437",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1438",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1435",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1439",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1440",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1442",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1441",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1443",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1444",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1445",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1446",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1447",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1448",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1449",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1450",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1451",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1452",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1453",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1454",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1457",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1455",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1456",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1463",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1458",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1464",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1461",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1459",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1462",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1460",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1471",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1465",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1469",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1466",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1467",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1472",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1475",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1476",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1481",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1479",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1478",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1474",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1473",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1470",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1480",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1468",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1477",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1482",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1483",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1484",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1485",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1486",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1488",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1489",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1490",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1491",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1492",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1493",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1494",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1495",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1497",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1498",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1499",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1500",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1501",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1502",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1503",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1496",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1487",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1504",
    "110ec58a-a0f2-4ac4-8393-c866d813b8d8-1505"
  ];

let results = {}; 
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

  async function runListOperation() {
    // if (S3_CONFIG.bucket === "YOUR_BUCKET_NAME" || S3_CONFIG.region === "YOUR_AWS_REGION") {
    //     console.error("Please fill in your S3 bucket name and AWS region in the S3_CONFIG object.");
    //     return;
    // }

    try {
        console.log("Starting list operation...");
        let startTime = Date.now();
        // 1. Get all object keys from the source prefix
        const sourceKeys = await listAllObjectKeys(S3_CONFIG.bucket, S3_CONFIG.sourcePrefix);

        if (sourceKeys.length === 0) {
            console.log("No objects found to move.");
            return;
        }

        let endTime = Date.now();
        console.log(`Total time taken: ${endTime - startTime} ms`);
        console.log(`Total objects found: ${sourceKeys.length}`);
        console.log("-----------------------------");
        results[S3_CONFIG.bucket] = sourceKeys?.length;
    } catch (err) {
        console.error("\nAn error occurred during the list operation:", err);
    }
}


for(let bucket of bucketList){
    // s3://110ec58a-a0f2-4ac4-8393-c866d813b8d8-1381/JITDeltaOAD-backup/GBValueAndDBCMappings/adsDataIPL2025JITDelta32/
    S3_CONFIG = {
        bucket: bucket, 
        sourcePrefix: "110ec58a-a0f2-4ac4-8393-c866d813b8d8#Account#0001/data/json/adsDataIPL2025JIT32/",           
        region: "us-east-1"          
    };  
    console.info(`Listing files from ${S3_CONFIG.bucket} with prefix ${S3_CONFIG.sourcePrefix}`);
    runListOperation();
    
    //wait for 3 seconds until next list operation
    await new Promise(resolve => setTimeout(resolve, 3*1000));
}

console.log(JSON.stringify(results));