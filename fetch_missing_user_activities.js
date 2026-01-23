const fs = require('fs');
const path = require('path');
const https = require('https');

// Configuration
const USER_ADDRESS = '0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d';
const DATA_DIR = '/Users/shubham.1/Applications/untitled folder/data/2026/01';
const VERBOSE = false;

// Helper function to make HTTP GET requests
function makeRequest(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let data = '';
      
      res.on('data', (chunk) => {
        data += chunk;
      });
      
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            resolve(data);
          }
        } else {
          reject(new Error(`HTTP ${res.statusCode}: ${data}`));
        }
      });
    }).on('error', (err) => {
      reject(err);
    });
  });
}

// Extract market slug from JSON file or filename
function extractMarketSlug(filePath, fileName) {
  try {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const data = JSON.parse(fileContent);
    
    if (data.marketSlug) {
      return data.marketSlug;
    }
  } catch (error) {
    // If JSON parsing fails, try to extract from filename
    if (fileName.includes('btc-updown-15m-')) {
      const match = fileName.match(/btc-updown-15m-(\d+)\.json/);
      if (match) {
        return `btc-updown-15m-${match[1]}`;
      }
    }
  }
  
  throw new Error('Could not extract market slug');
}

// Get condition ID from market slug
async function getConditionId(slug) {
  const url = `https://gamma-api.polymarket.com/markets/slug/${slug}`;
  
  try {
    const response = await makeRequest(url);
    
    let conditionId = null;
    
    if (response.conditionId) {
      conditionId = response.conditionId;
    } else if (response.condition && response.condition.id) {
      conditionId = response.condition.id;
    } else if (response.id) {
      conditionId = response.id;
    }
    
    if (!conditionId) {
      throw new Error('Could not find condition ID in API response');
    }
    
    return conditionId;
  } catch (error) {
    throw new Error(`Error fetching condition ID: ${error.message}`);
  }
}

// Fetch user activity until REDEEM transaction is found
async function fetchUserActivityUntilRedeem(conditionId, userAddress) {
  const limit = 1000;
  let offset = 0;
  let allTransactions = [];
  let foundRedeem = false;
  
  while (true) {
    try {
      const url = `https://data-api.polymarket.com/activity?limit=${limit}&sortBy=TIMESTAMP&sortDirection=ASC&market=${conditionId}&user=${userAddress}&offset=${offset}`;
      
      const transactions = await makeRequest(url);
      
      if (!Array.isArray(transactions) || transactions.length === 0) {
        break;
      }
      
      allTransactions = allTransactions.concat(transactions);
      
      // Check if last transaction is REDEEM
      const lastTransaction = transactions[transactions.length - 1];
      if (lastTransaction.type === 'REDEEM') {
        foundRedeem = true;
        break;
      }
      
      // If we got fewer transactions than the limit, we've reached the end
      if (transactions.length < limit) {
        break;
      }
      
      // Increment offset for next batch
      offset += limit;
      
      // Add a small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 500));
      
    } catch (error) {
      throw new Error(`Error fetching user activity: ${error.message}`);
    }
  }
  
  return {
    transactions: allTransactions,
    totalCount: allTransactions.length,
    foundRedeem: foundRedeem
  };
}

// Process a single file
async function processFile(fileName) {
  const filePath = path.join(DATA_DIR, fileName);
  
  try {
    // Extract market slug
    const marketSlug = extractMarketSlug(filePath, fileName);
    
    // Check if already processed
    const marketDir = path.join(DATA_DIR, marketSlug);
    const userActivityFile = path.join(marketDir, 'user_activity.json');
    if (fs.existsSync(userActivityFile)) {
      return { success: true, skipped: true, fileName, marketSlug };
    }
    
    console.log(`\n📄 Processing: ${fileName}`);
    console.log(`   Market Slug: ${marketSlug}`);
    
    // Get condition ID
    const conditionId = await getConditionId(marketSlug);
    
    // Fetch user activity
    console.log(`   Fetching user activity...`);
    const result = await fetchUserActivityUntilRedeem(conditionId, USER_ADDRESS);
    
    // Create a subdirectory for this market
    if (!fs.existsSync(marketDir)) {
      fs.mkdirSync(marketDir, { recursive: true });
    }
    
    // Copy the original data file to the market directory
    const dataFileInMarketDir = path.join(marketDir, fileName);
    if (!fs.existsSync(dataFileInMarketDir)) {
      fs.copyFileSync(filePath, dataFileInMarketDir);
    }
    
    // Save user activity results
    const outputFile = path.join(marketDir, 'user_activity.json');
    fs.writeFileSync(outputFile, JSON.stringify({
      marketSlug,
      conditionId,
      userAddress: USER_ADDRESS,
      totalTransactions: result.totalCount,
      foundRedeem: result.foundRedeem,
      transactions: result.transactions
    }, null, 2));
    
    console.log(`   ✓ Complete: ${result.totalCount} transactions, REDEEM: ${result.foundRedeem ? 'Yes' : 'No'}`);
    
    return {
      success: true,
      skipped: false,
      fileName,
      marketSlug,
      totalTransactions: result.totalCount,
      foundRedeem: result.foundRedeem
    };
    
  } catch (error) {
    console.error(`   ❌ Error: ${error.message}`);
    return {
      success: false,
      skipped: false,
      fileName,
      error: error.message
    };
  }
}

// Find files that need processing
function findFilesNeedingProcessing() {
  const allFiles = fs.readdirSync(DATA_DIR);
  const jsonFiles = allFiles.filter(file => 
    file.endsWith('.json') && 
    !file.includes('user_activity') &&
    fs.statSync(path.join(DATA_DIR, file)).isFile()
  );
  
  const filesToProcess = [];
  
  for (const jsonFile of jsonFiles) {
    try {
      const marketSlug = extractMarketSlug(
        path.join(DATA_DIR, jsonFile),
        jsonFile
      );
      
      const marketDir = path.join(DATA_DIR, marketSlug);
      const userActivityFile = path.join(marketDir, 'user_activity.json');
      
      if (!fs.existsSync(userActivityFile)) {
        filesToProcess.push(jsonFile);
      }
    } catch (error) {
      // If we can't extract slug, skip it
      console.log(`⚠️  Skipping ${jsonFile}: ${error.message}`);
    }
  }
  
  return filesToProcess;
}

// Main function
async function main() {
  console.log('='.repeat(60));
  console.log('Fetch Missing User Activities');
  console.log('='.repeat(60));
  console.log(`Data Directory: ${DATA_DIR}`);
  console.log(`User Address: ${USER_ADDRESS}`);
  console.log('='.repeat(60));
  
  try {
    const filesToProcess = findFilesNeedingProcessing();
    
    if (filesToProcess.length === 0) {
      console.log('\n✓ All files already have user_activity.json!');
      return;
    }
    
    console.log(`\nFound ${filesToProcess.length} file(s) needing processing\n`);
    
    const results = {
      total: filesToProcess.length,
      successful: 0,
      skipped: 0,
      failed: 0,
      details: []
    };
    
    // Process each file
    for (let i = 0; i < filesToProcess.length; i++) {
      const fileName = filesToProcess[i];
      console.log(`[${i + 1}/${filesToProcess.length}]`);
      
      const result = await processFile(fileName);
      results.details.push(result);
      
      if (result.skipped) {
        results.skipped++;
      } else if (result.success) {
        results.successful++;
      } else {
        results.failed++;
      }
      
      // Add delay between files to avoid rate limiting
      if (i < filesToProcess.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    // Final summary
    console.log('\n' + '='.repeat(60));
    console.log('FINAL SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total files: ${results.total}`);
    console.log(`✓ Successful: ${results.successful}`);
    console.log(`⏭️  Skipped: ${results.skipped}`);
    console.log(`❌ Failed: ${results.failed}`);
    
    if (results.failed > 0) {
      console.log('\nFailed files:');
      results.details
        .filter(r => !r.success && !r.skipped)
        .forEach(r => {
          console.log(`  - ${r.fileName}: ${r.error}`);
        });
    }
    
  } catch (error) {
    console.error('\n❌ Fatal Error:', error.message);
    process.exit(1);
  }
}

// Run the script
main();
