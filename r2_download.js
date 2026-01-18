const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream/promises");
require("dotenv").config();

const accountId = process.env.R2_ACCOUNT_ID;
const accessKeyId = process.env.R2_ACCESS_KEY_ID;
const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY;
const bucketName = process.env.R2_BUCKET_NAME;

const r2Client = new S3Client({
  region: "auto",
  endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId,
    secretAccessKey,
  },
});

async function downloadAll() {
  console.log(`Downloading all files from bucket '${bucketName}'...`);

  let isTruncated = true;
  let nextContinuationToken = null;

  try {
    while (isTruncated) {
      const listCommand = new ListObjectsV2Command({
        Bucket: bucketName,
        ContinuationToken: nextContinuationToken,
      });

      const { Contents, IsTruncated, NextContinuationToken } = await r2Client.send(listCommand);

      if (!Contents) {
        console.log("Bucket is empty.");
        break;
      }

      for (const obj of Contents) {
        const key = obj.Key;
        if (key.endsWith("/")) continue; // Skip directory markers

        const localPath = path.join(process.cwd(), key);
        fs.mkdirSync(path.dirname(localPath), { recursive: true });

        console.log(`Downloading: ${key}`);
        const getCommand = new GetObjectCommand({
          Bucket: bucketName,
          Key: key,
        });

        const { Body } = await r2Client.send(getCommand);
        await pipeline(Body, fs.createWriteStream(localPath));
      }

      isTruncated = IsTruncated;
      nextContinuationToken = NextContinuationToken;
    }
    console.log("Download complete! All files saved in current directory.");
  } catch (err) {
    console.error("Error during download:", err.message);
  }
}

downloadAll();
