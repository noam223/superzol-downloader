const { Client } = require('pg');
const admin = require('firebase-admin');
const serviceAccount = require('./firebase-service-account.json'); // שים כאן את הקובץ שהורדת מ-Firebase Console

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const firestore = admin.firestore();

const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

(async () => {
  await client.connect();

  // שלוף את כל הטבלאות הרלוונטיות (למשל כל טבלה שמתחילה ב-"products_")
  const res = await client.query(\`
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public' AND tablename LIKE 'products_%';
  \`);

  for (const row of res.rows) {
    const tableName = row.tablename;
    console.log(\`📥 Fetching data from \${tableName}...\`);

    const result = await client.query(\`SELECT * FROM \${tableName};\`);

    for (const item of result.rows) {
      const docId = item.product_id || item.item_code || Math.random().toString(36).substring(2, 12);
      await firestore
        .collection('products')
        .doc(tableName)
        .collection('items')
        .doc(docId)
        .set(item);
    }

    console.log(\`✅ Uploaded \${result.rows.length} items to Firestore collection: products/\${tableName}/items\`);
  }

  await client.end();
})();
