const fs = require('fs');
const path = require('path')

const OUTPUT_DIR = 'outputs'
const OUTPUT_FILE = 'output_categories.txt'
const TABLE = 'output_categories'

// Read the content of the JSON file
fs.readFile(path.join(OUTPUT_DIR, OUTPUT_FILE), 'utf8', (err, data) => {
  if (err) {
    console.error('Error reading file:', err);
    return;
  }

  try {
    const jsonData = JSON.parse(data);
    console.log(data)

    if (!Array.isArray(jsonData) || jsonData.length === 0) {
      console.error('Invalid or empty JSON array.');
      return;
    }

    // Get the properties from the first object in the JSON array
    const properties = Object.keys(jsonData[0]);
    console.log('properties', properties)

    // Generate SQL insert queries
    const insertQueries = jsonData.map((item) => {
      // const values = properties.map(prop =>  `'${item[prop]?.trim()}'`)
      const values = properties.map(prop => `'${item[prop]?.replace(/'/g, "''").trim()}'`);


      const columns = properties.join(', ');
      return `INSERT INTO ${TABLE} (${columns}) VALUES (${values});`;
    });

    // Write the insert queries into a text file
    fs.appendFile(`insert_queries.txt`, insertQueries.join('\n'), 'utf8', (err) => {
      if (err) {
        console.error('Error writing file:', err);
        return;
      }
      console.log('Insert queries have been written to insert_queries.txt');
    });
  } catch (error) {
    console.error('Error parsing JSON:', error);
  }
});
