// index.js – גרסה מלאה עם ניהול מבצעים ועדכון SSL
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'; // ⛔ ביטול בדיקת SSL עבור fetch

import fs from 'fs';
import zlib from 'zlib';
import { chromium } from 'playwright';
import { Client } from 'pg';
import { XMLParser } from 'fast-xml-parser';
import dotenv from 'dotenv';
dotenv.config();

const logins = JSON.parse(fs.readFileSync('./logins.json'));
const parser = new XMLParser({ ignoreAttributes: false });
const BASE_URL = 'https://url.publishedprices.co.il';

const getLatestFiles = (fileList) => {
  const map = new Map();
  for (const file of fileList) {
    const match = file.fname.match(/^(PriceFull|Price|PromoFull|Promo)(\d+)-(\d+)-\d{12}\.gz$/i);
    if (!match) continue;
    const [_, type, chainId, storeId] = match;
    const key = `${type.toLowerCase()}_${storeId}`;
    const existing = map.get(key);
    if (!existing || file.ftime > existing.ftime) {
      map.set(key, { ...file, type: type.toLowerCase(), chainId, storeId });
    }
  }
  return Array.from(map.values());
};

(async () => {
  const browser = await chromium.launch({
    headless: true,
    ignoreHTTPSErrors: true  // ✅ עוקף בעיות SSL באתר
  });

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();

  for (const { username, password } of logins) {
    console.log(`\n🔐 Logging in as ${username}...`);
    const context = await browser.newContext();
    const page = await context.newPage();

    try {
      await page.goto(`${BASE_URL}/login`);
      await page.fill('input[name="username"]', username);
      await page.fill('input[name="password"]', password || '');
      await Promise.all([
        page.waitForNavigation(),
        page.click('button[type="submit"]'),
      ]);

      const cookie = (await context.cookies()).find(c => c.name === 'cftpSID');
      const csrf = await page.getAttribute('meta[name="csrftoken"]', 'content');

      console.log(`📁 Fetching file list for ${username}...`);

      const res = await page.request.post(`${BASE_URL}/file/json/dir`, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
        form: {
          sEcho: '1', iColumns: '5', sColumns: ',,,,',
          iDisplayStart: '0', iDisplayLength: '100000',
          mDataProp_0: 'fname', mDataProp_1: 'typeLabel',
          mDataProp_2: 'size', mDataProp_3: 'ftime',
          mDataProp_4: '', sSearch: '', bRegex: 'false',
          iSortingCols: '0', cd: '/', csrftoken: csrf,
        },
      });

      const fileList = (await res.json()).aaData || [];
      const latestFiles = getLatestFiles(fileList);

      for (const { fname, type, chainId, storeId } of latestFiles) {
        const fileUrl = `${BASE_URL}/file/d/${fname}`;
        console.log(`⬇️ Downloading ${fname}`);

        try {
          const fetchRes = await import('node-fetch').then(mod => mod.default)(fileUrl, {
            headers: { Cookie: `cftpSID=${cookie.value}` },
          });

          const buffer = await fetchRes.buffer();
          const xml = zlib.gunzipSync(buffer).toString('utf8');
          const json = parser.parse(xml);
          const table = `products_${chainId}_${storeId}`;

          if (type.startsWith('price')) {
            let items = json?.Root?.Items?.Item || [];
            if (!Array.isArray(items)) items = [items];
            const storeName = json?.Root?.StoreName || 'Unknown';

            await client.query(`
              CREATE TABLE IF NOT EXISTS ${table} (
                product_id TEXT,
                store_id TEXT,
                chain_id TEXT,
                item_name TEXT,
                manufacturer_name TEXT,
                manufacturer_item_id TEXT,
                unit_qty TEXT,
                quantity REAL,
                unit_of_measure TEXT,
                b_is_weighted BOOLEAN,
                item_price REAL,
                unit_price REAL,
                price_update_date TEXT,
                PromotionId TEXT,
                PromotionDescription TEXT,
                PromotionUpdateDate TEXT,
                PromotionStartDate TEXT,
                PromotionStartHour TEXT,
                PromotionEndDate TEXT,
                PromotionEndHour TEXT,
                MinQty TEXT,
                DiscountedPrice TEXT,
                DiscountedPricePerMida TEXT,
                MinNoOfItemOfered TEXT,
                store_name TEXT
              );
            `);

            for (const item of items) {
              const values = [
                item.ItemCode, storeId, chainId, item.ItemName, item.ManufacturerName,
                item.ManufacturerItemDescription, item.UnitQty, parseFloat(item.Quantity || 0),
                item.UnitOfMeasure, item.bIsWeighted === '1', parseFloat(item.ItemPrice || 0),
                parseFloat(item.UnitOfMeasurePrice || 0), item.PriceUpdateDate || null,
                null, null, null, null, null, null, null, null, null, storeName
              ];

              await client.query(
                `INSERT INTO ${table} VALUES (${values.map((_, i) => `$${i + 1}`).join(',')})`,
                values
              );
            }
            console.log(`✅ Parsed file ${fname} for chain ${chainId}, store ${storeId}`);

          } else if (type.startsWith('promo')) {
            await client.query(`UPDATE ${table} SET
              PromotionId = NULL,
              PromotionDescription = NULL,
              PromotionUpdateDate = NULL,
              PromotionStartDate = NULL,
              PromotionStartHour = NULL,
              PromotionEndDate = NULL,
              PromotionEndHour = NULL,
              MinQty = NULL,
              DiscountedPrice = NULL,
              DiscountedPricePerMida = NULL,
              MinNoOfItemOfered = NULL
              WHERE PromotionId IS NOT NULL`);
            console.log(`🧹 Cleared old promotions from table ${table}`);

            let promotions = json?.Root?.Promotions?.Promotion || [];
            if (!Array.isArray(promotions)) promotions = [promotions];
            let updated = 0;
            for (const promo of promotions) {
              let products = promo?.PromotionItems?.Item || [];
              if (!Array.isArray(products)) products = [products];
              for (const product of products) {
                await client.query(`UPDATE ${table} SET
                  PromotionId = $1,
                  PromotionDescription = $2,
                  PromotionUpdateDate = $3,
                  PromotionStartDate = $4,
                  PromotionStartHour = $5,
                  PromotionEndDate = $6,
                  PromotionEndHour = $7,
                  MinQty = $8,
                  DiscountedPrice = $9,
                  DiscountedPricePerMida = $10,
                  MinNoOfItemOfered = $11
                  WHERE product_id = $12`, [
                  promo.PromotionId,
                  promo.PromotionDescription,
                  promo.PromotionUpdateDate,
                  promo.PromotionStartDate,
                  promo.PromotionStartHour,
                  promo.PromotionEndDate,
                  promo.PromotionEndHour,
                  promo.MinQty,
                  promo.DiscountedPrice,
                  promo.DiscountedPricePerMida,
                  promo.MinNoOfItemOfered,
                  product.ItemCode
                ]);
                updated++;
              }
            }
            console.log(`🔥 Updated ${updated} promotion items in ${table}`);
          }

        } catch (err) {
          console.error(`❌ Error in file ${fname}:`, err.message);
        }
      }

      await context.close();
    } catch (err) {
      console.error(`❌ Error with ${username}:`, err.message);
    }
  }

  await client.end();
  await browser.close();
  console.log('✅ All chains processed.');
})();
