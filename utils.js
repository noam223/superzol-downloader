import zlib from 'zlib';
import { parseStringPromise } from 'xml2js';

/**
 * בוחר את הקובץ העדכני ביותר מכל סוג וסניף
 */
function getLatestFiles(fileList, username) {
  const latestMap = new Map();

  for (const file of fileList) {
    const fname = file.fname || file[0];
    const match = fname.match(/^(PriceFull|Price|PromoFull|Promo|Stores).*?(\d{9})-(\d{3})/i);
    if (!match) continue;

    const [_, type, chainId, storeId] = match;
    const key = `${type}_${chainId}_${storeId}`;
    const ftime = new Date(file.ftime || file[3]).getTime();

    if (!latestMap.has(key) || ftime > latestMap.get(key).ftime) {
      latestMap.set(key, { fname, ftime });
    }
  }

  return Array.from(latestMap.values());
}

/**
 * מפרק קובץ XML או GZ, מכניס לטבלת הסניף הרלוונטי,
 * ואם הדגל true – גם לטבלה products_index
 */
async function parseAndInsertFile(buffer, fileMeta, pgClient, updateIndex = false) {
  const isGz = fileMeta.fname.endsWith('.gz');
  const rawXml = isGz ? zlib.gunzipSync(buffer).toString('utf8') : buffer.toString('utf8');

  const result = await parseStringPromise(rawXml);
  const root = result?.Root;
  if (!root?.Items?.[0]?.Item) return;

  const items = root.Items[0].Item;
  const storeId = root.StoreId?.[0];
  const chainId = root.ChainId?.[0];
  const storeName = root?.StoreName?.[0] || '';
  const tableName = `products_${chainId}_${storeId}`;

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      item_code TEXT,
      item_name TEXT,
      manufacturer_name TEXT,
      manufacturer_item_id TEXT,
      quantity TEXT,
      unit_of_measure TEXT,
      unit_qty TEXT,
      b_is_weighted TEXT,
      item_price REAL,
      unit_price REAL,
      price_update_date TEXT
    );
  `);

  for (const item of items) {
    const item_code = item.ItemCode?.[0] || item.ManufacturerItemID?.[0];
    const item_name = item.ItemName?.[0];
    const manufacturer_name = item.ManufacturerName?.[0] || 'לא ידוע';
    const quantity = item.Quantity?.[0];
    const unit_of_measure = item.UnitOfMeasure?.[0];
    const unit_qty = item.UnitQty?.[0];
    const b_is_weighted = item.BIsWeighted?.[0];
    const item_price = parseFloat(item.ItemPrice?.[0] || 0);
    const unit_price = parseFloat(item.UnitOfMeasurePrice?.[0] || 0);
    const price_update_date = item.PriceUpdateDate?.[0];
    const manufacturer_item_id = item.ManufacturerItemID?.[0];

    await pgClient.query(
      `INSERT INTO ${tableName} (
        item_code, item_name, manufacturer_name, manufacturer_item_id,
        quantity, unit_of_measure, unit_qty, b_is_weighted,
        item_price, unit_price, price_update_date
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [item_code, item_name, manufacturer_name, manufacturer_item_id,
       quantity, unit_of_measure, unit_qty, b_is_weighted,
       item_price, unit_price, price_update_date]
    );

    // הכנסת מוצר לטבלת index הכללית
    if (updateIndex && item_code) {
      const category = null; // אפשר להוסיף חישוב קטגוריה בהמשך
      await pgClient.query(
        `INSERT INTO products_index (
          item_code, item_name, manufacturer_name, category)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (item_code)
        DO UPDATE SET 
          item_name = EXCLUDED.item_name,
          manufacturer_name = EXCLUDED.manufacturer_name,
          category = EXCLUDED.category,
          updated_at = now()`,
        [item_code, item_name, manufacturer_name, category]
      );
    }
  }
}

export { getLatestFiles, parseAndInsertFile };
