const fs = require('fs');
const path = require('path');
const { parseStringPromise } = require('xml2js');
const { Client } = require('pg');
const zlib = require('zlib');

// 🧠 נתיב לקובץ ה־.gz
const filePath = './PriceFull7290058140886-028-202506250010.gz'; // עדכן לפי הצורך

// 🧠 חילוץ תאריך מהשם (YYYY-MM-DD HH:mm:00)
const extractDateFromFilename = (filename) => {
  const match = filename.match(/(\d{8})(\d{4})/);
  if (!match) return null;
  const [_, date, time] = match;
  return `${date.slice(0,4)}-${date.slice(4,6)}-${date.slice(6,8)} ${time.slice(0,2)}:${time.slice(2,4)}:00`;
};

const loadPriceFullToPostgres = async () => {
  const client = new Client({
    connectionString: process.env.DATABASE_URL, // מגיע אוטומטית מ־Railway
    ssl: { rejectUnauthorized: false }
  });

  await client.connect();

  // 🧠 שלב 1: קריאה ופיענוח
  const buffer = fs.readFileSync(filePath);
  const xml = zlib.gunzipSync(buffer).toString('utf8');
  const result = await parseStringPromise(xml);

  const items = result?.Root?.Items?.[0]?.Item || [];
  const chainId = result?.Root?.ChainId?.[0] || null;
  const storeId = result?.Root?.StoreId?.[0] || null;
  const updateDate = extractDateFromFilename(path.basename(filePath));

  console.log(`📦 Found ${items.length} items to insert`);

  // 🧠 שלב 2: לולאה והכנסה
  for (const item of items) {
    const values = {
      product_id: item.ItemCode?.[0] || null,
      store_id: storeId,
      chain_id: chainId,
      item_name: item.ItemName?.[0] || null,
      manufacturer_name: item.ManufacturerName?.[0] || null,
      manufacturer_item_id: item.ManufacturerItemDesc?.[0] || null,
      unit_qty: parseFloat(item.UnitQty?.[0] || 0),
      quantity: parseInt(item.Quantity?.[0] || 0),
      unit_of_measure: item.UnitOfMeasure?.[0] || null,
      b_is_weighted: item.bIsWeighted?.[0] === '1',
      item_price: parseFloat(item.ItemPrice?.[0] || 0),
      unit_price: parseFloat(item.BisPrice?.[0] || 0),
      update_date: updateDate,
    };

    await client.query(`
      INSERT INTO price_full (
        product_id, store_id, chain_id, item_name, manufacturer_name,
        manufacturer_item_id, unit_qty, quantity, unit_of_measure,
        b_is_weighted, item_price, unit_price, update_date
      )
      VALUES (
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9,
        $10, $11, $12, $13
      )
    `, Object.values(values));
  }

  await client.end();
  console.log(`✅ Loaded ${items.length} products into price_full`);
};

// ✅ הפעלה
loadPriceFullToPostgres().catch(err => {
  console.error('❌ Error:', err.message);
});
