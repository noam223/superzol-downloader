// קובץ ראשי שמבצע הורדה + פענוח + טעינה למסד
const { chromium } = require('playwright');
const zlib = require('zlib');
const { XMLParser } = require('fast-xml-parser');
const { Client } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const parser = new XMLParser({ ignoreAttributes: false });
const BASE_URL = 'https://url.publishedprices.co.il';
const username = 'politzer';
const password = '';

const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const getLatestFiles = (fileList) => {
  const map = new Map();

  for (const file of fileList) {
    const match = file.match(/^(pricefull|price|promofull|promo)(\d+)-(\d+)-([\d]{12})\.gz$/i);
    if (!match) continue;
    const [_, type, chainId, storeId, datetime] = match;
    const key = `${type}_${storeId}`;
    const existing = map.get(key);
    if (!existing || datetime > existing.datetime) {
      map.set(key, { file, type: type.toLowerCase(), chainId, storeId, datetime });
    }
  }

  return Array.from(map.values());
};

const fetchAndProcessFiles = async () => {
  await client.connect();
  const browser = await chromium.launch();
  const page = await browser.newPage();

  await page.goto(`${BASE_URL}/login`);
  await page.fill('input[name=username]', username);
  await page.fill('input[name=password]', password);
  await Promise.all([
    page.waitForNavigation(),
    page.click('button[type=submit']),
  ]);

  await page.goto(`${BASE_URL}/files`);
  const links = await page.$$eval('a', (els) =>
    els.map((el) => el.getAttribute('href')).filter((x) => x && /\.gz$/i.test(x))
  );

  const filtered = links.filter((name) =>
    /^(price|promo|pricefull|promofull)/i.test(name)
  );
  const latestFiles = getLatestFiles(filtered);

  for (const { file, type, chainId, storeId } of latestFiles) {
    const url = `${BASE_URL}/${file}`;
    console.log(`⬇️  Downloading: ${file}`);

    const response = await page.goto(url);
    const buffer = await response.body();
    const xml = zlib.gunzipSync(buffer).toString('utf8');
    const json = parser.parse(xml);

    const table = `products_${chainId}_${storeId}`;

    if (type === 'pricefull' || type === 'price') {
      const items = json?.Root?.Items?.Item || [];
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
    } else if (type === 'promofull' || type === 'promo') {
      const promotions = json?.Root?.Promotions?.Promotion || [];

      for (const promo of promotions) {
        const promoId = promo.PromotionId;
        const products = promo?.PromotionItems?.Item || [];

        for (const product of products) {
          await client.query(
            `UPDATE ${table} SET
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
            WHERE product_id = $12`,
            [
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
              product.ItemCode,
            ]
          );
        }
      }
    }
  }

  await client.end();
  await browser.close();
};

fetchAndProcessFiles().catch(console.error);
