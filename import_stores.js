const fs = require('fs');
const path = require('path');
const xml2js = require('xml2js');
const { Client } = require('pg');
require('dotenv').config();

// התחברות למסד הנתונים ב־Railway
const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});
await client.query(`
  CREATE TABLE IF NOT EXISTS stores (
    store_id TEXT PRIMARY KEY,
    store_name TEXT,
    chain_id TEXT,
    chain_name TEXT,
    address TEXT,
    city TEXT,
    zip_code TEXT
  );
`);
// תיקיית הבסיס של ההורדות
const baseDir = path.join(__dirname, 'downloads');

async function processXmlFile(filePath) {
  const xml = fs.readFileSync(filePath, 'utf8');
  const result = await xml2js.parseStringPromise(xml);
  const stores = result?.Stores?.Store || [];

  for (const s of stores) {
    const chainId = s.ChainId?.[0] || '';
    const storeId = s.StoreId?.[0] || '';
    const name = s.StoreName?.[0] || '';
    const address = s.Address?.[0] || '';
    const city = s.City?.[0] || '';

    await client.query(`
      INSERT INTO stores (chain_id, store_id, name, address, city)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (chain_id, store_id) DO UPDATE SET
        name = EXCLUDED.name,
        address = EXCLUDED.address,
        city = EXCLUDED.city
    `, [chainId, storeId, name, address, city]);

    console.log(`✅ ${chainId}-${storeId}: ${name}`);
  }
}

(async () => {
  await client.connect();
  console.log('🔗 Connected to PostgreSQL');

  const users = fs.readdirSync(baseDir).filter(f => fs.statSync(path.join(baseDir, f)).isDirectory());

  for (const user of users) {
    const userPath = path.join(baseDir, user);
    const files = fs.readdirSync(userPath).filter(f => f.startsWith('Stores') && f.endsWith('.xml'));

    for (const file of files) {
      const fullPath = path.join(userPath, file);
      console.log(`📦 Processing ${file} for ${user}...`);
      await processXmlFile(fullPath);
    }
  }

  await client.end();
  console.log('🎉 כל קבצי Stores נטענו למסד!');
})();
