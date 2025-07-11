import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import { chromium } from 'playwright';
import { Client } from 'pg';
import { XMLParser } from 'fast-xml-parser';
import dotenv from 'dotenv';
dotenv.config();

import logins from './logins.json' assert { type: 'json' };
const parser = new XMLParser({ ignoreAttributes: false });
const BASE_URL = 'https://url.publishedprices.co.il';

const getLatestFiles = (fileList) => {
  const map = new Map();
  for (const file of fileList) {
    const match = file.fname.match(/^(pricefull|price|promofull|promo)(\d+)-(\d+)-(\d{12})\.gz$/i);
    if (!match) continue;
    const [_, type, chainId, storeId, datetime] = match;
    const key = `${type}_${storeId}`;
    const existing = map.get(key);
    if (!existing || datetime > existing.datetime) {
      map.set(key, { ...file, type: type.toLowerCase(), chainId, storeId, datetime });
    }
  }
  return Array.from(map.values());
};

(async () => {
  const browser = await chromium.launch();
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
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
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
          csrftoken: csrf,
        },
      });

      const fileList = (await res.json()).aaData || [];
      const latestFiles = getLatestFiles(fileList);

      for (const { fname, type, chainId, storeId } of latestFiles) {
        const fileUrl = `${BASE_URL}/file/d/${fname}`;
        console.log(`⬇️ Downloading ${fname}`);

        const fetchRes = await fetch(fileUrl, {
          headers: {
            Cookie: `cftpSID=${cookie.value}`,
          },
        });

        const buffer = await fetchRes.arrayBuffer();
        const xml = zlib.gunzipSync(Buffer.from(buffer)).toString('utf8');
        const json = parser.parse(xml);

        console.log(`✅ Parsed file ${fname} for chain ${chainId}, store ${storeId}`);
      }

      await context.close();
    } catch (err) {
      console.error(`❌ Error with ${username}:`, err);
    }
  }

  await client.end();
  await browser.close();
  console.log('\n🎉 All chains processed!');
})();
