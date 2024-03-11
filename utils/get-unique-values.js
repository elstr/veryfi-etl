const fs = require('fs')
const path = require('path')

const DATA_FILE = '../veryfi_off_dataset.json'
const PROPS = ['categories_en', 'categories_tags']
const UNIQUE_PROP = 'categories_tags'
const UNIQUES_FILE = 'output_categories_2.txt'
const UNIQUES_DIR = 'utils/outputs'

function hasAllProperties(obj, props) {
  return props.every(prop => obj.hasOwnProperty(prop))
}

function readJsonFile(filePath) {
  fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) {
      console.error('Error reading the file:', err)
      return
    }

    try {
      const all = []
      const jsonData = JSON.parse(data)
      for (const element of jsonData) {
        if (hasAllProperties(element, PROPS)) {
          const propValues = PROPS.map(prop => element[prop].toLowerCase().split(','))
          const propObjects = propValues[0].map((_, i) => {
            const obj = {}
            PROPS.forEach((prop, index) => {
              obj[prop] = propValues[index][i]
            })
            return obj
          })
          all.push(...propObjects)
        }
      }

      const uniqueData = all.filter((obj, index, self) =>
        index === self.findIndex(t => t[UNIQUE_PROP] === obj[UNIQUE_PROP])
      )

      // Write uniques to a text file
      fs.writeFile(path.join(UNIQUES_DIR, UNIQUES_FILE), JSON.stringify(uniqueData, null, 2), err => {
        if (err) {
          console.error('Error writing to file:', err)
          return
        }
        console.log('Unique data has been written to', UNIQUES_FILE)
      })
    } catch (error) {
      console.error('Error parsing JSON:', error)
    }
  })
}

readJsonFile(DATA_FILE)
