export async function parseAndInsertFile(buffer, fileMeta, pgClient, updateIndex = false) {
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

  // יצירת טבלה לכל סניף אם לא קיימת
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

    // הכנסת לטבלה המרכזית
    if (updateIndex && item_code) {
      const category = null; // אפשר לחשב לפי item_name אם תרצה
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
