// utils.js – כולל תמיכה ב-GZIP אוטומטי ובשמירה לטבלת products_index

import path from 'path';
import zlib from 'zlib';
import { parseStringPromise } from 'xml2js';

function isGzip(buffer) {
  return buffer[0] === 0x1f && buffer[1] === 0x8b;
}

export async function getLatestFiles(fileList, username) {
  if (!Array.isArray(fileList)) return [];

  const grouped = {};

  for (const file of fileList) {
    const fname = file.fname;
    const match = fname.match(/(PriceFull|Price|PromoFull|Promo|Stores)(\d+_\d+|\d+)-/);
    if (!match) continue;
    const [_, type, storeId] = match;
    const key = `${type}_${storeId}`;

    if (!grouped[key] || grouped[key].ftime < file.ftime) {
      grouped[key] = { ...file, type, storeId, username };
    }
  }

  return Object.values(grouped);
}

export async function parseAndInsertFile(buffer, fileMeta, pgClient, insertToIndex = false) {
  const { fname, type, storeId, username } = fileMeta;
  const rawXml = isGzip(buffer)
    ? zlib.gunzipSync(buffer).toString('utf8')
    : buffer.toString('utf8');

  const result = await parseStringPromise(rawXml);

  if (type === 'Stores') {
    const stores = result.Stores.Store || [];
    for (const store of stores) {
      const store_id = store.StoreId?.[0];
      const store_name = store?.StoreName?.[0] || null;
      const chain_id = store?.ChainId?.[0] || username;
      const table = `products_${chain_id}_${store_id}`;

      await pgClient.query(`
        CREATE TABLE IF NOT EXISTS ${table} (
          item_code TEXT,
          item_name TEXT,
          manufacturer_name TEXT,
          quantity TEXT,
          unit_of_measure TEXT,
          unit_qty TEXT,
          b_is_weighted TEXT,
          item_price REAL,
          unit_price REAL,
          price_update_date TIMESTAMP,
          store_name TEXT,
          PRIMARY KEY(item_code)
        )
      `);
    }
    return;
  }

  const items = result?.PriceFull?.Items?.[0]?.Item ||
                result?.Price?.Items?.[0]?.Item ||
                result?.PromoFull?.Promotions?.[0]?.Item ||
                result?.Promo?.Promotions?.[0]?.Item || [];

  const chain_id = username;
  const table = `products_${chain_id}_${storeId}`;

  for (const item of items) {
    const item_code = item.ItemCode?.[0];
    const item_name = item.ItemName?.[0] || '';
    const manufacturer_name = item.ManufacturerName?.[0] || '';
    const quantity = item.Quantity?.[0] || null;
    const unit_of_measure = item.UnitOfMeasure?.[0] || null;
    const unit_qty = item.UnitQty?.[0] || null;
    const b_is_weighted = item.BIsWeighted?.[0] || null;
    const item_price = parseFloat(item.ItemPrice?.[0]) || null;
    const unit_price = parseFloat(item.UnitOfMeasurePrice?.[0]) || null;
    const price_update_date = item.PriceUpdateDate?.[0] || null;

    await pgClient.query(
      `INSERT INTO ${table} (
        item_code, item_name, manufacturer_name,
        quantity, unit_of_measure, unit_qty,
        b_is_weighted, item_price, unit_price,
        price_update_date, store_name
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      ON CONFLICT (item_code) DO UPDATE SET
        item_name = EXCLUDED.item_name,
        manufacturer_name = EXCLUDED.manufacturer_name,
        quantity = EXCLUDED.quantity,
        unit_of_measure = EXCLUDED.unit_of_measure,
        unit_qty = EXCLUDED.unit_qty,
        b_is_weighted = EXCLUDED.b_is_weighted,
        item_price = EXCLUDED.item_price,
        unit_price = EXCLUDED.unit_price,
        price_update_date = EXCLUDED.price_update_date,
        store_name = EXCLUDED.store_name
      `,
      [
        item_code, item_name, manufacturer_name,
        quantity, unit_of_measure, unit_qty,
        b_is_weighted, item_price, unit_price,
        price_update_date, storeId
      ]
    );

    if (insertToIndex && item_code) {
      await pgClient.query(
        `INSERT INTO products_index (
          item_code, item_name, manufacturer_name, category
        ) VALUES ($1,$2,$3,$4)
        ON CONFLICT (item_code) DO UPDATE SET
          item_name = EXCLUDED.item_name,
          manufacturer_name = EXCLUDED.manufacturer_name,
          category = EXCLUDED.category,
          updated_at = now()
        `,
        [item_code, item_name, manufacturer_name, null]
      );
    }
  }
}
