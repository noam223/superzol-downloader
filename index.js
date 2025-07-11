// index.js – גרסה מורחבת שמריצה על כל הרשתות מתוך logins.json

import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import { parseStringPromise } from 'xml2js';
import { Client } from 'pg';
import playwright from 'playwright';
import logins from './logins.json' assert { type: 'json' };
import { getLatestFiles, parseAndInsertFile } from './utils.js'; // פונקציות קיימות אצלך
import dotenv from 'dotenv';
dotenv.config();

const BASE_URL = 'https://url.publishedprices.co.il';

const pgClient = new Client({ connectionString: process.env.DATABASE_URL });
await pgClient.connect();

// ודא שקיימת הטבלה המאוחדת
await pgClient.query(`
  CREATE TABLE IF NOT EXISTS products_index (
    item_code TEXT PRIMARY KEY,
    item_name TEXT,
    manufacturer_name TEXT,
    category TEXT,
    updated_at TIMESTAMP DEFAULT now()
  );
`);

const browser = await playwright.chromium.launch({ headless: true });

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
      page.click('button[type="submit"]')
    ]);

    const cookie = (await context.cookies()).find(c => c.name === 'cftpSID');
    const csrf = await page.getAttribute('meta[name="csrftoken"]', 'content');

    console.log(`📁 Fetching file list for ${username}...`);
    const fileListRes = await page.request.post(`${BASE_URL}/file/json/dir`, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
      },
      form: {
        sEcho: '1',
        iColumns: '5',
        sColumns: ',,,,',
        iDisplayStart: '0',
        iDisplayLength: '100000',
        mDataProp_0: 'fname',
        mDataProp_1: 'typeLabel',
        mDataProp_2: 'size',
        mDataProp_3: 'ftime',
        mDataProp_4: '',
        sSearch: '',
        bRegex: 'false',
        iSortingCols: '0',
        cd: '/',
        csrftoken: csrf
      }
    });

    const fileListJson = await fileListRes.json();
    const fileList = Array.isArray(fileListJson?.aaData) ? fileListJson.aaData : [];

    const latestFiles = await getLatestFiles(fileList, username);
    if (!Array.isArray(latestFiles)) {
      console.warn(`⚠️ No valid files found for ${username}`);
      await context.close();
      continue;
    }

    for (const fileMeta of latestFiles) {
      try {
        console.log(`\n📥 Downloading ${fileMeta.fname} for ${username}...`);
        const fileRes = await page.request.get(`${BASE_URL}/file/${fileMeta.fname}`);
        const buffer = await fileRes.body();
        await parseAndInsertFile(buffer, fileMeta, pgClient, true); // הוספת דגל הכנסת מוצר לטבלה המאוחדת
      } catch (err) {
        console.error(`❌ Failed to process file ${fileMeta.fname} (${username}):`, err);
      }
    }

    await context.close();
  } catch (err) {
    console.error(`❌ Error with ${username}:`, err);
  }
}

await browser.close();
await pgClient.end();
console.log('\n✅ All chains processed.');
