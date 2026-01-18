const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

// R2 configuration from environment variables
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

async function uploadDir(localDir, prefix = "") {
  const entries = fs.readdirSync(localDir, { withFileTypes: true });

  for (let entry of entries) {
    const fullPath = path.join(localDir, entry.name);
    const key = prefix ? `${prefix}/${entry.name}` : entry.name;

    if (entry.isDirectory()) {
      await uploadDir(fullPath, key);
    } else if (entry.isFile()) {
      const fileContent = fs.readFileSync(fullPath);
      
      try {
        await r2Client.send(new PutObjectCommand({
          Bucket: bucketName,
          Key: key,
          Body: fileContent,
        }));
        console.log(`Uploaded: ${key}`);
      } catch (err) {
        console.error(`Error uploading ${key}:`, err.message);
      }
    }
  }
}

// Target directory from screenshot: polymarket/data/2025/12
const targetDir = path.join(process.cwd(), "polymarket", "data", "2025", "12");

if (!fs.existsSync(targetDir)) {
  console.error(`Error: Directory not found at ${targetDir}`);
  process.exit(1);
}

console.log(`Starting upload from ${targetDir} to bucket ${bucketName} root...`);
uploadDir(targetDir)
  .then(() => console.log("Upload complete!"))
  .catch((err) => console.error("Upload failed:", err));
