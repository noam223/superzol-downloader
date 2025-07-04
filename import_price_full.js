const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { parseStringPromise } = require('xml2js');
const { Client } = require('pg');

const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

function extractDateFromFilename(filename) {
  const parts = filename.split('-');
  return parts[3].replace('.gz', ''); // לדוג': 202507031130
}

function get(val) {
  return (val && val[0]) || null;
}

async function loadPriceFullFromFile(filePath, filename) {
  const compressedData = fs.readFileSync(filePath);
  const xmlData = zlib.gunzipSync(compressedData).toString('utf8');
  const result = await parseStringPromise(xmlData);

  const items = result?.Root?.Item || [];
  const updateDate = extractDateFromFilename(filename);

  for (const item of items) {
    await client.query(
      `INSERT INTO price_full (
        product_id, store_id, chain_id, item_name,
        manufacturer_name, manufacturer_item_id, unit_qty,
        quantity, unit_of_measure, b_is_weighted,
        item_price, unit_price, update_date
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [
        get(item.ItemCode),
        get(item.StoreId),
        get(item.ChainId),
        get(item.ItemName),
        get(item.ManufacturerName),
        get(item.ManufacturerItemCode),
        get(item.UnitQty),
        get(item.Quantity),
        get(item.UnitOfMeasure),
        get(item.bIsWeighted),
        get(item.ItemPrice),
        get(item.UnitPrice),
        updateDate
      ]
    );
  }

  console.log(`✅ ${filename} imported (${items.length} items)`);
}

async function importAllPriceFull() {
  const basePath = path.join(__dirname, 'downloads');
  await client.connect();

  const networks = fs.readdirSync(basePath);
  for (const net of networks) {
    const dir = path.join(basePath, net);
    const files = fs.readdirSync(dir).filter(f => f.startsWith('PriceFull') && f.endsWith('.gz'));

    for (const file of files) {
      const fullPath = path.join(dir, file);
      await loadPriceFullFromFile(fullPath, file);
    }
  }

  await client.end();
  console.log('🎉 All PriceFull files imported successfully!');
}

importAllPriceFull().catch(err => console.error('❌ Error:', err));
