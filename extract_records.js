import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Get __dirname equivalent for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Function to extract initial substring before first # and including #
function extractInitialSubstring(record) {
    if (typeof record !== 'string') return null;
    
    const hashIndex = record.indexOf('#');
    if (hashIndex === -1) return null;
    
    return record.substring(0, hashIndex + 1);
}

// Function to process a single JSON file
async function processJsonFile(filePath, uniqueSubstrings) {
    try {
        console.log(`Processing: ${path.basename(filePath)}`);
        
        // Read and parse JSON file
        const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        
        // Handle different JSON structures - could be array or object with array
        let records = [];
        if (Array.isArray(data)) {
            records = data;
        } else if (data.records && Array.isArray(data.records)) {
            records = data.records;
        } else if (typeof data === 'object') {
            // If it's an object, try to find arrays within it
            for (const key in data) {
                if (Array.isArray(data[key])) {
                    records = records.concat(data[key]);
                }
            }
        }
        
        // Process each record
        for (const record of records) {
            let recordString = '';
            
            // Convert record to string if it's not already
            if (typeof record === 'string') {
                recordString = record;
            } else if (typeof record === 'object' && record !== null) {
                // If it's an object, look for string values that might contain our pattern
                recordString = JSON.stringify(record);
            }
            
            // Extract only the initial substring from index 0 to first '#'
            const firstHashIndex = recordString.indexOf('#');
            if (firstHashIndex > 0) {
                const initialSubstring = recordString.substring(0, firstHashIndex);
                // Only add if it contains digits (to avoid extracting random text)
                if (/^\d+$/.test(initialSubstring)) {
                    uniqueSubstrings.add(initialSubstring);
                }
            }
        }
        
    } catch (error) {
        console.warn(`Warning: Could not process ${filePath}: ${error.message}`);
    }
}

// Main function to process all files
async function extractUniqueSubstrings() {
    const dataDir = path.join(__dirname, 'data', 'new');
    const uniqueSubstrings = new Set();
    
    try {
        // Check if directory exists
        if (!fs.existsSync(dataDir)) {
            console.error(`Directory not found: ${dataDir}`);
            return;
        }
        
        // Get all JSON files
        const files = fs.readdirSync(dataDir)
            .filter(file => file.endsWith('.json'))
            .map(file => path.join(dataDir, file));
        
        console.log(`Found ${files.length} JSON files to process`);
        
        // Process files with a limit to avoid memory issues
        const batchSize = 10;
        for (let i = 0; i < files.length; i += batchSize) {
            const batch = files.slice(i, i + batchSize);
            
            await Promise.all(
                batch.map(file => processJsonFile(file, uniqueSubstrings))
            );
            
            console.log(`Processed ${Math.min(i + batchSize, files.length)}/${files.length} files. Found ${uniqueSubstrings.size} unique substrings so far.`);
        }
        
        // Convert Set to Array and sort for better readability
        const sortedSubstrings = Array.from(uniqueSubstrings).sort((a, b) => {
            // Sort numerically by the number part
            const numA = parseInt(a, 10);
            const numB = parseInt(b, 10);
            return numA - numB;
        });
        
        console.log('\n=== UNIQUE INITIAL SUBSTRINGS ===');
        console.log(`Total unique substrings found: ${sortedSubstrings.length}`);
        console.log('All unique initial substrings:');
        sortedSubstrings.forEach((substring, index) => {
            console.log(`${index + 1}: ${substring}`);
        });
        
        // Save results to a file as well
        const outputFile = path.join(__dirname, 'unique_substrings.txt');
        fs.writeFileSync(outputFile, sortedSubstrings.join('\n'));
        console.log(`\nResults also saved to: ${outputFile}`);
        
    } catch (error) {
        console.error('Error processing files:', error);
    }
}

// Run the extraction
console.log('Starting extraction of unique initial substrings...');
extractUniqueSubstrings()
    .then(() => {
        console.log('Extraction completed!');
    })
    .catch(error => {
        console.error('Fatal error:', error);
    });