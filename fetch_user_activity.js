const fs = require('fs');
const path = require('path');
const https = require('https');

// Configuration
const USER_ADDRESS = '0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d'; // Change this to the user address you want to query
const DATA_DIR = '/Users/shubham.1/Applications/untitled folder/data/2026/01';
const PROCESS_ALL_FILES = true; // Set to false to process only FIRST_FILE
const FIRST_FILE = '2025-12-31_23-45-28_btc-updown-15m-1767224700.json';
const VERBOSE = false; // Set to true for detailed output

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

// Step 1: Extract market slug from JSON file
function extractMarketSlug(filePath) {
  try {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const data = JSON.parse(fileContent);
    
    if (data.marketSlug) {
      return data.marketSlug;
    }
    
    throw new Error('marketSlug not found in file');
  } catch (error) {
    throw new Error(`Error reading file: ${error.message}`);
  }
}

// Step 2: Get condition ID from market slug
async function getConditionId(slug) {
  const url = `https://gamma-api.polymarket.com/markets/slug/${slug}`;
  if (VERBOSE) {
    console.log(`\nFetching condition ID for slug: ${slug}`);
    console.log(`URL: ${url}`);
  }
  
  try {
    const response = await makeRequest(url);
    
    // The condition ID is typically in the conditionId field or condition.id
    let conditionId = null;
    
    if (response.conditionId) {
      conditionId = response.conditionId;
    } else if (response.condition && response.condition.id) {
      conditionId = response.condition.id;
    } else if (response.id) {
      conditionId = response.id;
    }
    
    if (!conditionId) {
      if (VERBOSE) {
        console.log('Full API response:', JSON.stringify(response, null, 2));
      }
      throw new Error('Could not find condition ID in API response');
    }
    
    if (VERBOSE) {
      console.log(`Condition ID: ${conditionId}`);
    }
    return conditionId;
  } catch (error) {
    throw new Error(`Error fetching condition ID: ${error.message}`);
  }
}

// Step 3: Fetch user activity until REDEEM transaction is found
async function fetchUserActivityUntilRedeem(conditionId, userAddress) {
  const limit = 1000;
  let offset = 0;
  let allTransactions = [];
  let foundRedeem = false;
  
  if (VERBOSE) {
    console.log(`\nFetching user activity for user: ${userAddress}`);
    console.log(`Market condition ID: ${conditionId}`);
  }
  
  while (!foundRedeem) {
    const url = `https://data-api.polymarket.com/activity?limit=${limit}&sortBy=TIMESTAMP&sortDirection=ASC&market=${conditionId}&user=${userAddress}&offset=${offset}`;
    
    if (VERBOSE) {
      console.log(`\nFetching batch: offset=${offset}, limit=${limit}`);
      console.log(`URL: ${url}`);
    }
    
    try {
      const response = await makeRequest(url);
      
      // Handle different response formats
      let transactions = [];
      if (Array.isArray(response)) {
        transactions = response;
      } else if (response.data && Array.isArray(response.data)) {
        transactions = response.data;
      } else if (response.results && Array.isArray(response.results)) {
        transactions = response.results;
      } else {
        if (VERBOSE) {
          console.log('Unexpected response format:', JSON.stringify(response, null, 2));
        }
        throw new Error('Unexpected API response format');
      }
      
      if (VERBOSE) {
        console.log(`Received ${transactions.length} transactions in this batch`);
      } else {
        process.stdout.write(`  Batch ${Math.floor(offset/limit) + 1}: ${transactions.length} transactions... `);
      }
      
      if (transactions.length === 0) {
        if (VERBOSE) {
          console.log('No more transactions found. Stopping search.');
        }
        break;
      }
      
      allTransactions = allTransactions.concat(transactions);
      
      // Check if the last transaction is REDEEM
      const lastTransaction = transactions[transactions.length - 1];
      if (lastTransaction.type === 'REDEEM') {
        if (VERBOSE) {
          console.log('\n✓ Found REDEEM transaction!');
          console.log('Last transaction:', JSON.stringify(lastTransaction, null, 2));
        } else {
          console.log('✓ REDEEM found!');
        }
        foundRedeem = true;
        break;
      }
      
      // If we got fewer transactions than the limit, we've reached the end
      if (transactions.length < limit) {
        if (VERBOSE) {
          console.log('Reached end of transactions (less than limit returned).');
          console.log('Last transaction type:', lastTransaction.type);
        } else {
          console.log(`End reached (last type: ${lastTransaction.type})`);
        }
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
    // Step 1: Extract market slug
    const marketSlug = extractMarketSlug(filePath);
    
    // Check if already processed
    const marketDir = path.join(DATA_DIR, marketSlug);
    const userActivityFile = path.join(marketDir, 'user_activity.json');
    if (fs.existsSync(userActivityFile)) {
      console.log(`⏭️  Skipping ${fileName} (already processed)`);
      return { success: true, skipped: true, fileName, marketSlug };
    }
    
    console.log(`\n📄 Processing: ${fileName}`);
    console.log(`   Market Slug: ${marketSlug}`);
    
    // Step 2: Get condition ID
    const conditionId = await getConditionId(marketSlug);
    
    // Step 3: Fetch user activity
    console.log(`   Fetching user activity...`);
    const result = await fetchUserActivityUntilRedeem(conditionId, USER_ADDRESS);
    
    // Create a subdirectory for this market
    if (!fs.existsSync(marketDir)) {
      fs.mkdirSync(marketDir, { recursive: true });
    }
    
    // Copy the original data file to the market directory
    const dataFileName = fileName;
    const dataFileInMarketDir = path.join(marketDir, dataFileName);
    
    if (!fs.existsSync(dataFileInMarketDir)) {
      fs.copyFileSync(filePath, dataFileInMarketDir);
    }
    
    // Save user activity results to file in the market directory
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
    console.error(`   ❌ Error processing ${fileName}: ${error.message}`);
    return {
      success: false,
      skipped: false,
      fileName,
      error: error.message
    };
  }
}

// Main function
async function main() {
  console.log('='.repeat(60));
  console.log('User Activity Fetcher - Batch Processing');
  console.log('='.repeat(60));
  console.log(`Data Directory: ${DATA_DIR}`);
  console.log(`User Address: ${USER_ADDRESS}`);
  console.log(`Processing: ${PROCESS_ALL_FILES ? 'All files' : 'Single file'}`);
  console.log('='.repeat(60));
  
  try {
    let filesToProcess = [];
    
    if (PROCESS_ALL_FILES) {
      // Get all JSON files in the directory
      const allFiles = fs.readdirSync(DATA_DIR);
      filesToProcess = allFiles.filter(file => 
        file.endsWith('.json') && 
        !file.includes('user_activity') &&
        fs.statSync(path.join(DATA_DIR, file)).isFile()
      );
      filesToProcess.sort();
    } else {
      filesToProcess = [FIRST_FILE];
    }
    
    console.log(`\nFound ${filesToProcess.length} file(s) to process\n`);
    
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
