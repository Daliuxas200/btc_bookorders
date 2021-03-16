const fs = require('fs');

function load() {
  return JSON.parse(fs.readFileSync('config.json', 'utf8'));
}

module.exports = load;