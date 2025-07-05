const fs = require('fs');
const path = require('path');
const { parseStringPromise } = require('xml2js');
const { Client } = require('pg');
const zlib = require('zlib');

const DOWNLOADS_DIR = path.join(__dirname, 'downloads');

const loadAllPriceFullFiles = async () => {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  await client.connect();

  const chains = fs.readdirSync(DOWNLOADS_DIR).filter((f) =>
    fs.statSync(path.join(DOWNLOADS_DIR, f)).isDirectory()
  );

  for (const chain of chains) {
    const chainDir = path.join(DOWNLOADS_DIR, chain);
    const files = fs.readdirSync(chainDir).filter((f) => f.startsWith('PriceFull') && f.endsWith('.gz'));

    for (const file of files) {
      const filePath = path.join(chainDir, file);
      console.log(`📂 Processing ${filePath}`);

      try {
        const buffer = fs.readFileSync(filePath);
        const xml = zlib.gunzipSync(buffer).toString('utf8');
        const result = await parseStringPromise(xml);

        const items = result?.Root?.Items?.[0]?.Item || [];
        const chainId = result?.Root?.ChainId?.[0] || null;
        const storeId = result?.Root?.StoreId?.[0] || null;

        console.log(`🔎 Found ${items.length} items`);

        for (const item of items) {
          const values = {
            product_id: item.ItemCode?.[0] || null,
            store_id: storeId,
            chain_id: chainId,
            item_name: item.ItemName?.[0] || null,
            manufacturer_name: item.ManufacturerName?.[0] || null,
            manufacturer_item_id: item.ManufacturerItemDescription?.[0] || null,
            unit_qty: item.UnitQty?.[0] || null,
            quantity: parseFloat(item.Quantity?.[0] || 0),
            unit_of_measure: item.UnitOfMeasure?.[0] || null,
            b_is_weighted: item.bIsWeighted?.[0] === '1',
            item_price: parseFloat(item.ItemPrice?.[0] || 0),
            unit_price: parseFloat(item.UnitOfMeasurePrice?.[0] || 0),
            price_update_date: item.PriceUpdateDate?.[0] || null,
          };

          await client.query(
            `
            INSERT INTO price_full_clean (
              product_id, store_id, chain_id, item_name, manufacturer_name,
              manufacturer_item_id, unit_qty, quantity, unit_of_measure,
              b_is_weighted, item_price, unit_price, price_update_date
            ) VALUES (
              $1, $2, $3, $4, $5,
              $6, $7, $8, $9,
              $10, $11, $12, $13, $14
            )
            `,
            Object.values(values)
          );
        }
      } catch (err) {
        console.error(`❌ Failed ${file}: ${err.message}`);
      }
    }
  }

  await client.end();
  console.log('✅ Done loading all PriceFull files');
};

loadAllPriceFullFiles().catch((err) => {
  console.error('❌ Fatal:', err.message);
});
